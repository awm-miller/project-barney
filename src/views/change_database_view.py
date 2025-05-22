import flet as ft
from pathlib import Path
from typing import List

from src.config import DATABASES_DIR
from src.utils.db_utils import load_known_databases, add_known_database
from src.utils.ui_utils import update_active_db_display # Assuming open_database is in main or passed to page

# Placeholder for open_database - this logic might need to live in main.py or be passed to the page object
# For now, we'll assume the page object has a method to handle opening a database.
def open_database_on_page(page: ft.Page, db_path_str: str):
    db_path = Path(db_path_str)
    if hasattr(page, 'active_db_path'): # Check if page has this attribute
        page.active_db_path = db_path
        if hasattr(page, 'active_db_chip_ref'): # Check for chip ref
            update_active_db_display(page.active_db_chip_ref, db_path)
        if hasattr(page, 'save_last_opened_db'): # Check for save function
            page.save_last_opened_db(db_path) # Save on successful open
        
        page.snack_bar = ft.SnackBar(ft.Text(f"Opened database: {db_path.name}"), open=True)
        page.update()
    else:
        print(f"Error: page object does not have expected attributes for opening database.")
        page.snack_bar = ft.SnackBar(ft.Text(f"Error opening {db_path.name}"), open=True, bgcolor="errorcontainer")
        page.update()

def build_change_database_view(page: ft.Page):
    available_databases_column = ft.Column(scroll=ft.ScrollMode.AUTO, spacing=5)
    
    def populate_databases_list():
        available_databases_column.controls.clear()
        known_dbs = load_known_databases()
        if not known_dbs:
            available_databases_column.controls.append(
                ft.Text("No databases found. Add one below or run the Pipeline Wizard.")
            )
        else:
            for db_path_obj in known_dbs:
                is_active = hasattr(page, 'active_db_path') and page.active_db_path == db_path_obj
                db_card = ft.Card(
                    content=ft.Container(
                        content=ft.Row(
                            [
                                ft.Icon("storage", color="primary" if is_active else None),
                                ft.Text(db_path_obj.name, weight=ft.FontWeight.BOLD if is_active else ft.FontWeight.NORMAL, expand=True),
                                ft.IconButton(
                                    icon="folder_open",
                                    tooltip="Open Database",
                                    on_click=lambda _, p=str(db_path_obj): open_database_on_page(page, p)
                                )
                            ],
                            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                            vertical_alignment=ft.CrossAxisAlignment.CENTER
                        ),
                        padding=10,
                        width=500 # Fixed width for cards
                    ),
                    elevation=2,
                    margin=ft.margin.only(bottom=5)
                )
                available_databases_column.controls.append(db_card)
        available_databases_column.update()

    # Callback for page to refresh this view (e.g., after a new DB is created by wizard)
    if hasattr(page, 'selected_db_changed_callback'): # Ensure page has this attribute
        page.selected_db_changed_callback = populate_databases_list 
    else:
        print("Warning: Page object does not have selected_db_changed_callback attribute.")

    def pick_db_file_result(e: ft.FilePickerResultEvent):
        if e.files:
            selected_file = e.files[0].path
            if selected_file.endswith(".db"):
                db_path_to_add = Path(selected_file)
                add_known_database(db_path_to_add, page) # Pass page for callback
                populate_databases_list() # Refresh the list
                page.snack_bar = ft.SnackBar(ft.Text(f"Added database: {db_path_to_add.name}"), open=True)
            else:
                page.snack_bar = ft.SnackBar(ft.Text("Please select a valid .db file."), open=True, bgcolor="errorcontainer")
        page.update()

    file_picker = ft.FilePicker(on_result=pick_db_file_result)
    page.overlay.append(file_picker) # Needs to be in overlay

    populate_databases_list()

    return ft.Column(
        [
            ft.Text("Manage Databases", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM, weight=ft.FontWeight.BOLD),
            ft.Text("Select an existing database or add a new one."),
            ft.Divider(),
            ft.Text("Available Databases:", theme_style=ft.TextThemeStyle.TITLE_MEDIUM),
            available_databases_column,
            ft.Divider(),
            ft.ElevatedButton(
                "Add Existing Database File (.db)",
                icon="add",
                on_click=lambda _: file_picker.pick_files(
                    allow_multiple=False,
                    allowed_extensions=["db"]
                )
            )
        ],
        expand=True,
        spacing=10,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        scroll=ft.ScrollMode.ADAPTIVE
    ) 