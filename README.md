# Realtime Vibe

A .NET MAUI Blazor Hybrid POC for `gpt-realtime-2`.

## Run

Set `OPENAI_API_KEY`, or paste your key into the app.

Windows:

```powershell
dotnet build -t:Run
```

Mac Catalyst:

```bash
dotnet workload install maui
dotnet build -t:Run
```

## Notes

- The C# client connects to `wss://api.openai.com/v1/realtime?model=gpt-realtime-2`.
- The Blazor WebView JavaScript only handles microphone capture and PCM playback.
- Audio is sent as mono 24 kHz little-endian PCM16 chunks.
- Push-to-talk uses manual `input_audio_buffer.commit` plus `response.create`.
- The app stores an API key only if the local "Remember key" checkbox is selected.
