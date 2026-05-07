using Microsoft.AspNetCore.Components.WebView;

#if WINDOWS
using Microsoft.Web.WebView2.Core;
#endif

namespace RealtimeVibe;

public partial class MainPage : ContentPage
{
	public MainPage()
	{
		InitializeComponent();
	}

	private void OnBlazorWebViewInitialized(object? sender, BlazorWebViewInitializedEventArgs e)
	{
#if WINDOWS
		e.WebView.CoreWebView2.PermissionRequested += (_, args) =>
		{
			if (args.PermissionKind == CoreWebView2PermissionKind.Microphone)
			{
				args.State = CoreWebView2PermissionState.Allow;
			}
		};
#endif
	}
}
