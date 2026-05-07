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
    private readonly HashSet<string> _seenUserTranscriptItems = [];

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
        _seenUserTranscriptItems.Clear();

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

        return SendEventAsync(new
        {
            type = "input_audio_buffer.append",
            audio = base64Pcm16
        });
    }

    public async Task ClearInputAudioAsync()
    {
        await SendEventAsync(new { type = "input_audio_buffer.clear" });
    }

    public async Task SendTextAsync(string text)
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

        await CreateTextResponseAsync();
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
            ["turn_detection"] = new
            {
                type = "server_vad",
                threshold = 0.5,
                prefix_padding_ms = 300,
                silence_duration_ms = 500,
                create_response = true,
                interrupt_response = true
            },
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
                        type = "audio/pcm",
                        rate = 24000
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

    private Task CreateTextResponseAsync()
    {
        var response = new Dictionary<string, object?>
        {
            ["output_modalities"] = new[] { "text" }
        };

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
        EmitClientEvent(json);

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
                    var errorMessage = ExtractError(root);
                    logger.LogWarning("Realtime API error: {Error}", errorMessage);
                    Emit(RealtimeNotificationKind.Error, errorMessage, eventType, rawJson: json);
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
                    EmitUserTranscriptOnce(root, eventType, TryGetString(root, "transcript") ?? TryGetString(root, "text"), json);
                    break;

                case "conversation.item.input_audio_transcription.segment":
                    Emit(RealtimeNotificationKind.ServerEvent, TryGetString(root, "text") ?? eventType, eventType, rawJson: json);
                    break;

                case "conversation.item.input_audio_transcription.failed":
                    Emit(RealtimeNotificationKind.Error, ExtractError(root), eventType, rawJson: json);
                    break;

                case "conversation.item.done":
                    EmitUserTranscriptOnce(root, eventType, ExtractUserTranscript(root), json);
                    break;

                case "response.done":
                    Emit(RealtimeNotificationKind.ResponseDone, "Response complete", eventType, ExtractResponseText(root), json);
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
            var message = TryGetString(error, "message");
            var code = TryGetString(error, "code");
            var param = TryGetString(error, "param");
            var type = TryGetString(error, "type");

            return string.Join(" ", new[]
                {
                    code,
                    param is null ? null : $"({param})",
                    message,
                    type is null ? null : $"[{type}]"
                }
                .Where(part => !string.IsNullOrWhiteSpace(part)))
                .Trim();
        }

        return root.ToString();
    }

    private void EmitUserTranscriptOnce(JsonElement root, string eventType, string? transcript, string json)
    {
        if (string.IsNullOrWhiteSpace(transcript))
        {
            return;
        }

        var itemId = TryGetString(root, "item_id") ?? TryGetNestedString(root, "item", "id");
        if (!string.IsNullOrWhiteSpace(itemId) && !_seenUserTranscriptItems.Add(itemId))
        {
            return;
        }

        Emit(RealtimeNotificationKind.UserTranscript, "user", eventType, transcript.Trim(), json);
    }

    private static string? ExtractUserTranscript(JsonElement root)
    {
        if (!root.TryGetProperty("item", out var item)
            || !StringEquals(item, "role", "user")
            || !item.TryGetProperty("content", out var content))
        {
            return null;
        }

        return ExtractTextFromContent(content);
    }

    private static string? ExtractResponseText(JsonElement root)
    {
        if (!root.TryGetProperty("response", out var response)
            || !response.TryGetProperty("output", out var output)
            || output.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        var parts = new List<string>();
        foreach (var item in output.EnumerateArray())
        {
            if (item.TryGetProperty("content", out var content))
            {
                var text = ExtractTextFromContent(content);
                if (!string.IsNullOrWhiteSpace(text))
                {
                    parts.Add(text);
                }
            }
        }

        return parts.Count == 0 ? null : string.Join(Environment.NewLine, parts).Trim();
    }

    private static string? ExtractTextFromContent(JsonElement content)
    {
        if (content.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        var parts = new List<string>();
        foreach (var part in content.EnumerateArray())
        {
            var text = TryGetString(part, "text") ?? TryGetString(part, "transcript");
            if (!string.IsNullOrWhiteSpace(text))
            {
                parts.Add(text);
            }
        }

        return parts.Count == 0 ? null : string.Join(Environment.NewLine, parts).Trim();
    }

    private static bool StringEquals(JsonElement element, string propertyName, string expected)
    {
        return string.Equals(TryGetString(element, propertyName), expected, StringComparison.OrdinalIgnoreCase);
    }

    private static string? TryGetNestedString(JsonElement element, string parentPropertyName, string childPropertyName)
    {
        return element.TryGetProperty(parentPropertyName, out var parent)
            ? TryGetString(parent, childPropertyName)
            : null;
    }

    private void EmitClientEvent(string json)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            var eventType = TryGetString(document.RootElement, "type") ?? "client.event";

            if (eventType == "input_audio_buffer.append")
            {
                return;
            }

            logger.LogDebug("Realtime client event {EventType}: {Payload}", eventType, json);
            Emit(RealtimeNotificationKind.ClientEvent, $"send {eventType}", eventType, rawJson: json);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Unable to parse outbound Realtime event.");
        }
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
