using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace RealtimeVibe.Services;

public sealed class RealtimeClient(ILogger<RealtimeClient> logger) : IAsyncDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private ClientWebSocket? _socket;
    private CancellationTokenSource? _sessionCts;
    private RealtimeSessionOptions? _options;
    private bool _audioAppended;

    public event Action<RealtimeClientNotification>? NotificationReceived;

    public bool IsConnected => _socket?.State == WebSocketState.Open;

    public async Task ConnectAsync(RealtimeSessionOptions options, CancellationToken cancellationToken = default)
    {
        if (IsConnected)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(options.ApiKey))
        {
            throw new InvalidOperationException("An OpenAI API key is required.");
        }

        _options = options with
        {
            ApiKey = options.ApiKey.Trim(),
            Model = string.IsNullOrWhiteSpace(options.Model) ? "gpt-realtime-2" : options.Model.Trim(),
            Voice = string.IsNullOrWhiteSpace(options.Voice) ? "marin" : options.Voice.Trim()
        };

        _sessionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var socket = new ClientWebSocket();
        socket.Options.SetRequestHeader("Authorization", $"Bearer {_options.ApiKey}");

        if (!string.IsNullOrWhiteSpace(_options.SafetyIdentifier))
        {
            socket.Options.SetRequestHeader("OpenAI-Safety-Identifier", _options.SafetyIdentifier);
        }

        var endpoint = new Uri($"wss://api.openai.com/v1/realtime?model={Uri.EscapeDataString(_options.Model)}");
        Emit(RealtimeNotificationKind.Status, $"Connecting to {_options.Model}...");
        await socket.ConnectAsync(endpoint, _sessionCts.Token);

        _socket = socket;
        _ = Task.Run(() => ReceiveLoopAsync(socket, _sessionCts.Token));

        Emit(RealtimeNotificationKind.Status, "Connected");
        await SendSessionUpdateAsync(_options, _sessionCts.Token);
    }

    public async Task DisconnectAsync()
    {
        var socket = _socket;
        _socket = null;
        _audioAppended = false;

        if (socket is null)
        {
            return;
        }

        try
        {
            _sessionCts?.Cancel();

            if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Client disconnected", timeout.Token);
            }
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Error while closing Realtime socket.");
        }
        finally
        {
            socket.Dispose();
            _sessionCts?.Dispose();
            _sessionCts = null;
            Emit(RealtimeNotificationKind.Status, "Disconnected");
        }
    }

    public Task AppendAudioAsync(string base64Pcm16)
    {
        if (string.IsNullOrWhiteSpace(base64Pcm16))
        {
            return Task.CompletedTask;
        }

        _audioAppended = true;
        return SendEventAsync(new
        {
            type = "input_audio_buffer.append",
            audio = base64Pcm16
        });
    }

    public async Task ClearInputAudioAsync()
    {
        _audioAppended = false;
        await SendEventAsync(new { type = "input_audio_buffer.clear" });
    }

    public async Task CommitAudioAndRespondAsync()
    {
        if (!_audioAppended)
        {
            Emit(RealtimeNotificationKind.Error, "No audio reached the client before stop.");
            return;
        }

        await SendEventAsync(new { type = "input_audio_buffer.commit" });
        _audioAppended = false;
        await CreateResponseAsync(speak: true);
    }

    public async Task SendTextAsync(string text, bool speak)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return;
        }

        await SendEventAsync(new
        {
            type = "conversation.item.create",
            item = new
            {
                type = "message",
                role = "user",
                content = new[]
                {
                    new
                    {
                        type = "input_text",
                        text
                    }
                }
            }
        });

        await CreateResponseAsync(speak);
    }

    private async Task SendSessionUpdateAsync(RealtimeSessionOptions options, CancellationToken cancellationToken)
    {
        var input = new Dictionary<string, object?>
        {
            ["format"] = new
            {
                type = "audio/pcm",
                rate = 24000
            },
            ["turn_detection"] = null,
            ["noise_reduction"] = new
            {
                type = "near_field"
            }
        };

        if (options.EnableInputTranscription)
        {
            input["transcription"] = new
            {
                model = "gpt-realtime-whisper"
            };
        }

        var session = new Dictionary<string, object?>
        {
            ["type"] = "realtime",
            ["model"] = options.Model,
            ["output_modalities"] = new[] { "audio" },
            ["instructions"] = options.Instructions,
            ["audio"] = new
            {
                input,
                output = new
                {
                    format = new
                    {
                        type = "audio/pcm"
                    },
                    voice = options.Voice
                }
            }
        };

        await SendEventAsync(new
        {
            type = "session.update",
            session
        }, cancellationToken);
    }

    private Task CreateResponseAsync(bool speak)
    {
        var response = new Dictionary<string, object?>
        {
            ["output_modalities"] = new[] { speak ? "audio" : "text" }
        };

        if (speak && _options is not null)
        {
            response["audio"] = new
            {
                output = new
                {
                    format = new
                    {
                        type = "audio/pcm"
                    },
                    voice = _options.Voice
                }
            };
        }

        return SendEventAsync(new
        {
            type = "response.create",
            response
        });
    }

    private async Task SendEventAsync(object payload, CancellationToken cancellationToken = default)
    {
        var socket = _socket;
        if (socket?.State != WebSocketState.Open)
        {
            throw new InvalidOperationException("Realtime socket is not connected.");
        }

        var json = JsonSerializer.Serialize(payload, JsonOptions);
        var bytes = Encoding.UTF8.GetBytes(json);

        await _sendLock.WaitAsync(cancellationToken);
        try
        {
            await socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, cancellationToken);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private async Task ReceiveLoopAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        var buffer = new byte[64 * 1024];
        using var message = new MemoryStream();

        try
        {
            while (!cancellationToken.IsCancellationRequested && socket.State == WebSocketState.Open)
            {
                var result = await socket.ReceiveAsync(buffer, cancellationToken);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    Emit(RealtimeNotificationKind.Status, "Server closed the connection.");
                    break;
                }

                message.Write(buffer, 0, result.Count);

                if (!result.EndOfMessage)
                {
                    continue;
                }

                var json = Encoding.UTF8.GetString(message.GetBuffer(), 0, (int)message.Length);
                message.SetLength(0);
                ProcessServerEvent(json);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on disconnect.
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Realtime receive loop failed.");
            Emit(RealtimeNotificationKind.Error, ex.Message);
        }
    }

    private void ProcessServerEvent(string json)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            var eventType = TryGetString(root, "type") ?? "server.event";

            Emit(RealtimeNotificationKind.ServerEvent, eventType, eventType, rawJson: json);

            switch (eventType)
            {
                case "error":
                    Emit(RealtimeNotificationKind.Error, ExtractError(root), eventType, rawJson: json);
                    break;

                case "response.output_audio.delta":
                case "response.audio.delta":
                    Emit(RealtimeNotificationKind.AudioDelta, "audio", eventType, TryGetString(root, "delta"), json);
                    break;

                case "response.output_audio_transcript.delta":
                case "response.audio_transcript.delta":
                case "response.output_text.delta":
                case "response.text.delta":
                    Emit(RealtimeNotificationKind.AssistantDelta, "assistant", eventType, TryGetString(root, "delta"), json);
                    break;

                case "conversation.item.input_audio_transcription.completed":
                    Emit(RealtimeNotificationKind.UserTranscript, "user", eventType, TryGetString(root, "transcript") ?? TryGetString(root, "text"), json);
                    break;

                case "conversation.item.input_audio_transcription.segment":
                    Emit(RealtimeNotificationKind.UserTranscript, "user", eventType, TryGetString(root, "text"), json);
                    break;

                case "response.done":
                    Emit(RealtimeNotificationKind.ResponseDone, "Response complete", eventType, rawJson: json);
                    break;
            }
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Unable to parse server event: {Json}", json);
            Emit(RealtimeNotificationKind.ServerEvent, "Unparsed server event", rawJson: json);
        }
    }

    private static string ExtractError(JsonElement root)
    {
        if (root.TryGetProperty("error", out var error))
        {
            return TryGetString(error, "message")
                ?? TryGetString(error, "code")
                ?? error.ToString();
        }

        return root.ToString();
    }

    private static string? TryGetString(JsonElement element, string propertyName)
    {
        return element.TryGetProperty(propertyName, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;
    }

    private void Emit(
        RealtimeNotificationKind kind,
        string message,
        string? eventType = null,
        string? delta = null,
        string? rawJson = null)
    {
        NotificationReceived?.Invoke(new RealtimeClientNotification(kind, message, eventType, delta, rawJson));
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync();
        _sendLock.Dispose();
    }
}
