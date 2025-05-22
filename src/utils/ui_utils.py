import flet as ft
from pathlib import Path
from typing import Dict

def update_active_db_display(active_db_chip_ref: ft.Ref[ft.Chip], db_path: Path | None):
    """Updates the database display Chip."""
    if active_db_chip_ref.current: 
        db_chip = active_db_chip_ref.current
        
        if db_path: 
            db_display_name = db_path.stem 
            db_chip.leading = ft.Icon(name="storage", color="primary", size=18)
            if not isinstance(db_chip.label, ft.Text):
                db_chip.label = ft.Text(db_display_name, size=12, overflow=ft.TextOverflow.ELLIPSIS, no_wrap=True)
            else:
                db_chip.label.value = db_display_name
                db_chip.label.size = 12
                db_chip.label.overflow = ft.TextOverflow.ELLIPSIS
                db_chip.label.no_wrap = True
            
            db_chip.tooltip = f"Active database: {db_display_name}"
        else: 
            db_chip.leading = ft.Row(
                [
                    ft.Icon(name="storage_outlined", opacity=0.7, size=18), 
                    ft.Icon(name="cancel_outlined", color="error", size=18, tooltip="No database selected")
                ], 
                spacing=4, 
                vertical_alignment=ft.CrossAxisAlignment.CENTER
            )
            if not isinstance(db_chip.label, ft.Text):
                db_chip.label = ft.Text("")
            else:
                db_chip.label.value = ""
            db_chip.tooltip = "No database active"
        
        db_chip.visible = True 
        db_chip.update()

def show_video_details_dialog(page: ft.Page, video_data: Dict):
    """ Shows video details, using ft.AlertDialog via page.overlay. """
    print(f"DEBUG: show_video_details_dialog (AlertDialog via overlay) for: {video_data.get('video_id', 'N/A')}")
    
    dialog_instance_ref = ft.Ref[ft.AlertDialog]()

    def close_the_alert_dialog(e):
        print(f"DEBUG: close_the_alert_dialog called. Dialog ref: {dialog_instance_ref.current}")
        if dialog_instance_ref.current:
            print(f"DEBUG: Dialog open state BEFORE close: {dialog_instance_ref.current.open}")
            dialog_instance_ref.current.open = False
            print(f"DEBUG: Dialog open state AFTER setting to False: {dialog_instance_ref.current.open}")
            page.update()
            print(f"DEBUG: page.update() called in close_the_alert_dialog.")

    transcript_content_text = "Transcript not available."
    subtitle_path_str = video_data.get('subtitle_file_path')

    if subtitle_path_str:
        try:
            subtitle_path = Path(subtitle_path_str)
            if subtitle_path.is_file():
                with open(subtitle_path, 'r', encoding='utf-8') as f:
                    transcript_content_text = f.read()
                if not transcript_content_text.strip():
                    transcript_content_text = "Transcript file is empty."
            else:
                transcript_content_text = f"Transcript file not found at: {subtitle_path_str}"
        except Exception as e:
            transcript_content_text = f"Error loading transcript: {e}"
            print(f"Error reading subtitle file {subtitle_path_str}: {e}")
    else:
        transcript_content_text = "No transcript path provided in database."

    ai_analysis_panel = ft.ExpansionPanel(
        header=ft.Container(ft.Text("AI Analysis", style=ft.TextThemeStyle.TITLE_MEDIUM), padding=ft.padding.only(left=10, top=5, bottom=5)),
        content=ft.Container(
            ft.Text(video_data.get('ai_analysis_content', 'No AI analysis available.'), selectable=True),
            padding=ft.padding.all(10)
        ),
    )

    transcript_panel = ft.ExpansionPanel(
        header=ft.Container(ft.Text("Transcript", style=ft.TextThemeStyle.TITLE_MEDIUM), padding=ft.padding.only(left=10, top=5, bottom=5)),
        content=ft.Container(
            ft.Text(transcript_content_text, selectable=True),
            padding=ft.padding.all(10),
            height=150, 
        ),
    )

    dialog_content_column = ft.Column(
        [
            ft.ExpansionPanelList(
                controls=[
                    ai_analysis_panel,
                    transcript_panel
                ],
                elevation=1,
                divider_color="outlinevariant"
            )
        ],
        tight=True, 
        width=600, 
        scroll=ft.ScrollMode.ADAPTIVE 
    )

    alert_dialog = ft.AlertDialog(
        ref=dialog_instance_ref, 
        modal=True,
        title=ft.Row(
            [
                ft.Text(video_data.get('title', 'Video Details'), weight=ft.FontWeight.BOLD, expand=True, overflow=ft.TextOverflow.ELLIPSIS, max_lines=2),
                ft.IconButton("close", on_click=close_the_alert_dialog, tooltip="Close dialog")
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER
        ),
        content=dialog_content_column, 
        shape=ft.RoundedRectangleBorder(radius=10), 
        actions=None,
        actions_alignment=ft.MainAxisAlignment.END,
        open=False 
    )
    
    if alert_dialog not in page.overlay:
        page.overlay.append(alert_dialog)

    alert_dialog.open = True
    page.update()

    print(f"DEBUG: AlertDialog added to overlay and opened. Open state: {alert_dialog.open}")

def check_active_db_and_show_snackbar(page: ft.Page) -> bool:
    if not hasattr(page, 'active_db_path') or not page.active_db_path:
        page.snack_bar = ft.SnackBar(
            ft.Text("No active database. Please select or create one first."), 
            open=True, 
            bgcolor="errorcontainer"
        )
        page.update()
        return False
    return True 