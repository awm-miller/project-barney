import flet as ft
import os
from pathlib import Path
from dotenv import load_dotenv

# This assumes the view is in src/views and .env is in project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent 

def build_pipeline_playlist_view(page: ft.Page):
    print(f"DEBUG: Building Pipeline Playlist View. DB Name: {getattr(page, 'pipeline_db_name', 'Not Set')}, Playlist URL: {getattr(page, 'pipeline_playlist_url', 'Not Set')}")
    playlist_id_ref = ft.Ref[ft.TextField]()
    api_key_ref = ft.Ref[ft.TextField]()

    load_dotenv(PROJECT_ROOT / ".env")
    youtube_api_key_env = os.getenv("YOUTUBE_API_KEY", "")

    def validate_and_save_playlist_info():
        playlist_id = playlist_id_ref.current.value.strip()
        api_key = api_key_ref.current.value.strip()
        print(f"DEBUG: Playlist Setup - Validating Playlist URL: '{playlist_id}', API Key: '{bool(api_key)}'")

        if not playlist_id:
            playlist_id_ref.current.error_text = "Playlist URL or ID cannot be empty."
            playlist_id_ref.current.update()
            return False
        
        # Basic validation (can be improved)
        if "list=" not in playlist_id and not (len(playlist_id) > 10 and playlist_id.startswith(("PL", "UU", "FL", "OL", "RD"))):
            if not ("youtube.com/playlist?list=" in playlist_id or "youtu.be/" in playlist_id):
                playlist_id_ref.current.error_text = "Please enter a valid YouTube Playlist URL or ID (e.g., PL...)."
                playlist_id_ref.current.update()
                return False

        playlist_id_ref.current.error_text = ""
        playlist_id_ref.current.update()
        page.pipeline_playlist_url = playlist_id
        page.pipeline_api_key = api_key
        print(f"DEBUG: Playlist Setup - Saved page.pipeline_playlist_url: '{page.pipeline_playlist_url}', page.pipeline_api_key set: '{bool(page.pipeline_api_key)}'")
        return True

    def on_start_processing_click(e):
        print("DEBUG: Playlist Setup - Start Processing clicked.")
        if validate_and_save_playlist_info():
            page.go("/pipeline_progress")

    def on_back_click(e):
        print("DEBUG: Playlist Setup - Back clicked. Navigating to /pipeline_db_setup")
        page.go("/pipeline_db_setup")

    # Restore values if already set
    if hasattr(page, 'pipeline_playlist_url') and page.pipeline_playlist_url:
        if playlist_id_ref.current:
            playlist_id_ref.current.value = page.pipeline_playlist_url
    if hasattr(page, 'pipeline_api_key') and page.pipeline_api_key:
        if api_key_ref.current:
            api_key_ref.current.value = page.pipeline_api_key
    elif youtube_api_key_env: # Pre-fill from .env if not already in page context
         if api_key_ref.current:
            api_key_ref.current.value = youtube_api_key_env

    youtube_key_field = ft.TextField(
        ref=api_key_ref,
        label="YouTube API Key (Optional)",
        hint_text="Paste your YouTube Data API key here",
        width=400,
        password=True,
        can_reveal_password=True,
        helper_text="Needed for very large playlists or if yt-dlp direct fetch fails.",
        prefix_icon="key",
        value=getattr(page, 'pipeline_api_key', youtube_api_key_env) # Pre-fill
    )

    return ft.Column(
        [
            ft.Text("Step 2: Playlist Information", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM, weight=ft.FontWeight.BOLD),
            ft.Text("Provide the YouTube Playlist URL or ID."),
            ft.Container(height=10),
            ft.TextField(
                ref=playlist_id_ref,
                label="YouTube Playlist URL or ID",
                hint_text="e.g., PLxxxxxxxxxxxxxxxxxxxxxx or full URL",
                width=400,
                prefix_icon="link",
                autofocus=True,
                value=getattr(page, 'pipeline_playlist_url', ''), # Pre-fill
                on_submit=lambda _: on_start_processing_click(None)
            ),
            ft.Container(height=10),
            youtube_key_field,
            ft.Container(height=20),
            ft.Row(
                [
                    ft.OutlinedButton(
                        "Back",
                        icon="arrow_back",
                        on_click=on_back_click
                    ),
                    ft.FilledButton(
                        "Start Processing", 
                        icon="play_arrow",
                        on_click=on_start_processing_click,
                        tooltip="This will run the script to create the database and fetch subtitles."
                    )
                ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                width=400
            )
        ],
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        alignment=ft.MainAxisAlignment.CENTER,
        spacing=15,
        expand=True,
        scroll=ft.ScrollMode.ADAPTIVE,
    ) 