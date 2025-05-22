import flet as ft
from pathlib import Path
from src.utils.ui_utils import update_active_db_display

def build_pipeline_complete_view(page: ft.Page):
    final_db_path = getattr(page, 'pipeline_final_db_path', None)
    print(f"DEBUG: Building Pipeline Complete View. Final DB Path: {final_db_path}")

    message = "Pipeline processing completed."
    details = "The database has been created and subtitles fetched (if any)."
    icon_name = "check_circle"
    icon_color = "green"  # Simple color name

    if not final_db_path:
        message = "Pipeline Completed with Issues"
        details = "Could not determine the final database path. Please check the logs in the previous step or try again."
        icon_name = "warning_amber_rounded"
        icon_color = "amber"  # Simple color name

    def on_view_database_click(e):
        print(f"DEBUG: Complete View - View Database clicked. DB Path: {final_db_path}")
        if final_db_path:
            page.active_db_path = Path(final_db_path)
            if page.active_db_chip_ref:
                 update_active_db_display(page.active_db_chip_ref, page.active_db_path)
            # Potentially save as last opened DB
            if hasattr(page, 'save_last_opened_db') and callable(page.save_last_opened_db):
                page.save_last_opened_db(str(page.active_db_path))
            
            # Check if the database preview view should be used or main view_database
            # For now, let's assume /view_database is the general one.
            page.go("/view_database") 
        else:
            print("DEBUG: Complete View - View Database: No final_db_path found.")
            page.show_snack_bar(ft.SnackBar(ft.Text("Error: No database path available to view."), open=True))

    def on_go_home_click(e):
        print("DEBUG: Complete View - Go Home clicked.")
        page.go("/")

    def on_try_again_click(e):
        print("DEBUG: Complete View - Try Again clicked. Clearing pipeline state.")
        # Clear pipeline state before going back to intro
        if hasattr(page, 'pipeline_db_name'): del page.pipeline_db_name
        if hasattr(page, 'pipeline_playlist_url'): del page.pipeline_playlist_url
        if hasattr(page, 'pipeline_api_key'): del page.pipeline_api_key
        if hasattr(page, 'pipeline_final_db_path'): del page.pipeline_final_db_path
        page.go("/pipeline_intro")

    view_button = ft.FilledButton(
        "View Created Database",
        icon="table_chart",
        on_click=on_view_database_click,
        disabled=not final_db_path
    )
    
    home_button = ft.OutlinedButton("Go to Home", icon="home", on_click=on_go_home_click)
    try_again_button = ft.TextButton("Run New Pipeline", icon="refresh", on_click=on_try_again_click)

    return ft.Column(
        [
            ft.Icon(name=icon_name, color=icon_color, size=60),
            ft.Container(height=10),
            ft.Text(message, theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM, weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER),
            ft.Container(height=5),
            ft.Text(details, text_align=ft.TextAlign.CENTER, size=14),
            ft.Container(height=30),
            view_button,
            ft.Container(height=10),
            ft.Row(
                [home_button, try_again_button],
                alignment=ft.MainAxisAlignment.CENTER, spacing=20
            )
        ],
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        alignment=ft.MainAxisAlignment.CENTER,
        spacing=15,
        expand=True
    ) 