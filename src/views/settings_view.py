import flet as ft
import os

# GEMINI_API_KEY will be read from environment or a config file in a real app
# For now, direct os.getenv and saving back to os.environ for simplicity in this view.

def build_settings_view(page: ft.Page):
    """Builds the settings view."""
    api_key_field_ref = ft.Ref[ft.TextField]()
    
    # Load existing key to display
    current_api_key = os.getenv("GEMINI_API_KEY", "")

    def save_api_key_click(e):
        """Saves the API key to environment variable (for this session)."""
        api_key = api_key_field_ref.current.value if api_key_field_ref.current else ""
        if api_key:
            os.environ["GEMINI_API_KEY"] = api_key # This sets it for the current process
            # Inform the user. A more robust solution would save to a config file or secure storage.
            page.snack_bar = ft.SnackBar(
                content=ft.Text("API key saved for this session. Restart may be required for other modules to see it if they load it at startup."),
                open=True
            )
            # Update global variable in run_ai_analysis_view if it's already imported and uses it directly
            # This is a bit of a hack due to direct os.getenv at module load in other views.
            # A better approach is a central config service or passing the key explicitly.
            try:
                from . import run_ai_analysis_view # Relative import if in the same package
                run_ai_analysis_view.GEMINI_API_KEY = api_key
            except ImportError:
                print("Could not update GEMINI_API_KEY in run_ai_analysis_view directly.")

        else:
            page.snack_bar = ft.SnackBar(
                content=ft.Text("API key field is empty."),
                open=True,
                bgcolor="warningcontainer"
            )
        page.update()
    
    return ft.Column(
        [
            ft.Text("Settings", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM, weight=ft.FontWeight.BOLD),
            ft.Container(height=20),  # Spacer
            ft.Text("API Configuration", theme_style=ft.TextThemeStyle.TITLE_MEDIUM),
            ft.TextField(
                ref=api_key_field_ref,
                label="Gemini API Key",
                password=True,
                can_reveal_password=True,
                value=current_api_key, # Display current key
                width=400,
                hint_text="Enter your Gemini API Key here"
            ),
            ft.FilledButton(
                "Save API Key",
                icon="save",
                on_click=save_api_key_click,
                tooltip="Saves the API key as an environment variable for the current session."
            ),
            ft.Text(
                "Note: API key is stored as an environment variable for the current session only. "
                "For persistent storage, consider setting it in your system's environment variables or a .env file.",
                italic=True,
                size=12,
                width=400,
                text_align=ft.TextAlign.CENTER
            )
        ],
        spacing=20,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        alignment=ft.MainAxisAlignment.CENTER, # Center content if view is short
        expand=True # Ensure it expands to fill space if needed
    ) 