import flet as ft
import re

def build_pipeline_db_setup_view(page: ft.Page):
    print(f"DEBUG: Building Pipeline DB Setup View. Current pipeline_db_name: {getattr(page, 'pipeline_db_name', 'Not Set')}")
    db_name_ref = ft.Ref[ft.TextField]()

    def validate_and_save_db_name():
        db_name = db_name_ref.current.value.strip()
        print(f"DEBUG: DB Setup - Validating DB name: '{db_name}'")
        if not db_name:
            db_name_ref.current.error_text = "Database name cannot be empty."
            db_name_ref.current.update()
            return False
        if not re.match(r"^[a-zA-Z0-9_\-]+$", db_name):
            db_name_ref.current.error_text = "Use only letters, numbers, underscores, or hyphens."
            db_name_ref.current.update()
            return False
        
        db_name_ref.current.error_text = ""
        db_name_ref.current.update()
        page.pipeline_db_name = db_name # Store in page context
        print(f"DEBUG: DB Setup - Saved page.pipeline_db_name: '{page.pipeline_db_name}'")
        return True

    def on_next_click(e):
        print("DEBUG: DB Setup - Next clicked.")
        if validate_and_save_db_name():
            page.go("/pipeline_playlist_setup")

    def on_back_click(e):
        print("DEBUG: DB Setup - Back clicked. Navigating to /pipeline_intro")
        page.go("/pipeline_intro") # Or use the main wizard route if that's preferred for intro

    # Restore value if already set
    if hasattr(page, 'pipeline_db_name') and page.pipeline_db_name:
        if db_name_ref.current:
            db_name_ref.current.value = page.pipeline_db_name

    return ft.Column(
        [
            ft.Text("Step 1: Database Details", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM, weight=ft.FontWeight.BOLD),
            ft.Text("Enter a name for your new database. This will be created in the 'databases' folder."),
            ft.Container(height=10),
            ft.TextField(
                ref=db_name_ref,
                label="Database Name",
                hint_text="e.g., my_channel_videos",
                width=400,
                prefix_icon="database",
                helper_text="Do not include .db extension. Alphanumeric and underscores preferred.",
                border_color="primary",
                autofocus=True,
                value=getattr(page, 'pipeline_db_name', ''), # Pre-fill if exists
                on_submit=lambda _: on_next_click(None)
            ),
            ft.Container(height=20),
            ft.Row(
                [
                    ft.OutlinedButton(
                        "Back",
                        icon="arrow_back",
                        on_click=on_back_click
                    ),
                    ft.FilledButton(
                        "Next",
                        icon="arrow_forward",
                        on_click=on_next_click
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