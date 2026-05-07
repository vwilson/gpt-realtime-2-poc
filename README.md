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

## Releases

GitHub Actions builds Windows and Mac Catalyst artifacts on pushes and pull requests.

To create a GitHub Release with downloadable builds, push a version tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow produces:

- `realtime-vibe-windows-x64.zip`
- `realtime-vibe-macos-maccatalyst-x64.zip`
- `realtime-vibe-macos-maccatalyst-arm64.zip`

The Mac Catalyst builds are unsigned and not notarized. They are useful for sharing a POC, but a polished public Mac distribution should add Apple Developer ID signing and notarization.

## Notes

- The C# client connects to `wss://api.openai.com/v1/realtime?model=gpt-realtime-2`.
- The Blazor WebView JavaScript only handles microphone capture and PCM playback.
- Audio is sent as mono 24 kHz little-endian PCM16 chunks.
- The microphone runs as a hot mic; server-side VAD creates turns and responses.
- The app stores an API key only if the local "Remember key" checkbox is selected.
