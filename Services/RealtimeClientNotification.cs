namespace RealtimeVibe.Services;

public enum RealtimeNotificationKind
{
    Status,
    ServerEvent,
    AssistantDelta,
    UserTranscript,
    AudioDelta,
    Error,
    ResponseDone
}

public sealed record RealtimeClientNotification(
    RealtimeNotificationKind Kind,
    string Message,
    string? EventType = null,
    string? Delta = null,
    string? RawJson = null);
