namespace RealtimeVibe.Services;

public sealed record RealtimeSessionOptions
{
    public string ApiKey { get; init; } = string.Empty;

    public string Model { get; init; } = "gpt-realtime-2";

    public string Voice { get; init; } = "marin";

    public string Instructions { get; init; } =
        "You are a sharp, warm voice assistant demoing GPT Realtime 2. Keep replies conversational, brief, and useful. If the user asks for a demo, show off low-latency voice, careful reasoning, and reliable instruction following.";

    public bool EnableInputTranscription { get; init; } = true;

    public string SafetyIdentifier { get; init; } = "realtime-vibe-poc";
}
