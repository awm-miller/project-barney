import sys
from pathlib import Path

# Add project root to sys.path to allow absolute imports from src
PROJECT_ROOT_FOR_PATH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT_FOR_PATH))

import flet as ft
from pathlib import Path
from typing import Optional

# Configuration and Utilities
from src.config import (
    APP_NAME,
    DATABASES_DIR, 
    APP_DATA_DIR,  
    KNOWN_DATABASES_FILE,
    LAST_OPENED_DB_FILE,
    PROJECT_ROOT 
)
from src.utils.db_utils import (
    load_last_opened_db,
    save_last_opened_db 
)
from src.utils.ui_utils import (
    update_active_db_display,
)

# Import View Builders
from src.views.home_view import build_home_view
# from src.views.pipeline_wizard_view import build_pipeline_wizard_view # Will be replaced
from src.views.change_database_view import build_change_database_view, open_database_on_page
from src.views.view_database_view import build_view_database_view
from src.views.run_ai_analysis_view import build_run_ai_analysis_view
from src.views.settings_view import build_settings_view
from src.views.database_preview_view import build_database_preview_view

# New Pipeline Wizard View Imports
from src.views.pipeline_intro_view import build_pipeline_intro_view
from src.views.pipeline_db_setup_view import build_pipeline_db_setup_view
from src.views.pipeline_playlist_view import build_pipeline_playlist_view
from src.views.pipeline_progress_view import build_pipeline_progress_view
from src.views.pipeline_complete_view import build_pipeline_complete_view

# --- Global UI Refs ---
# These refs are for components that might be updated from various parts of the app,
# often passed via the 'page' object to views.
page_ref = ft.Ref[ft.Page]()
active_db_chip_ref = ft.Ref[ft.Chip]()
main_content_area_ref = ft.Ref[ft.Column]()

# --- Ensure essential directories and files exist ---
# This should run once at startup.
DATABASES_DIR.mkdir(parents=True, exist_ok=True)
APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
if not KNOWN_DATABASES_FILE.exists():
    KNOWN_DATABASES_FILE.touch()
if not LAST_OPENED_DB_FILE.exists():
    LAST_OPENED_DB_FILE.touch()

# --- Main Application Setup ---
# These global-like variables are necessary because Flet's main function initializes the UI tree,
# and these components are part of that tree or define its behavior.
main_content_column: Optional[ft.Column] = None
nav_rail: Optional[ft.NavigationRail] = None
route_to_view_builder_map: dict = {}

def app_main(page: ft.Page): # Renamed from main to avoid conflict if this file is imported
    global main_content_column, nav_rail, route_to_view_builder_map
    
    page.title = APP_NAME
    page.theme_mode = ft.ThemeMode.DARK # Or ft.ThemeMode.LIGHT
    page.vertical_alignment = ft.MainAxisAlignment.START 
    page.horizontal_alignment = ft.CrossAxisAlignment.START 
    
    # Store global refs and essential properties/methods on the page object.
    # This makes them accessible to view builders more cleanly.
    page_ref.current = page 
    page.active_db_path: Optional[Path] = None
    page.active_db_chip_ref = active_db_chip_ref 
    page.main_content_area_ref = main_content_area_ref # For views that might need to access it (though discouraged)
    page.save_last_opened_db = save_last_opened_db # Make save function accessible for opening DBs
    
    # Placeholder for a callback that the Change DB view can use to refresh itself
    # if a database is added externally (e.g., by the pipeline wizard).
    page.selected_db_changed_callback = lambda: None 

    # Define the routes and their corresponding view builders
    route_to_view_builder_map = {
        "/": build_home_view,
        # "/pipeline_wizard": build_pipeline_wizard_view, # Old route
        "/pipeline_intro": build_pipeline_intro_view,        # New intro route
        "/pipeline_db_setup": build_pipeline_db_setup_view,
        "/pipeline_playlist_setup": build_pipeline_playlist_view,
        "/pipeline_progress": build_pipeline_progress_view,
        "/pipeline_complete": build_pipeline_complete_view,
        "/change_database": build_change_database_view,
        "/view_database": build_view_database_view,
        "/ai_analysis": build_run_ai_analysis_view,
        "/settings": build_settings_view,
        "/database_preview": build_database_preview_view, # Still useful for direct preview
    }
    
    # --- UI Component Definitions ---
    
    # Handler for the database display chip click
    def chip_on_click_handler(e):
        page.go("/change_database")

    db_display_chip = ft.Chip(
        ref=active_db_chip_ref,
        label=ft.Text("No DB Active"), # Initial sensible label
        leading=ft.Icon("storage_outlined"), # Corrected icon
        visible=True, 
        on_click=chip_on_click_handler,
        tooltip="Click to change active database"
    )

    nav_idx_to_route_map = {
        0: "/", 
        1: "/pipeline_intro", # Changed from /pipeline_wizard
        2: "/view_database",
        3: "/ai_analysis", 
        4: "/settings",
    }
    
    def nav_rail_on_change(e: ft.ControlEvent):
        selected_idx = e.control.selected_index
        route = nav_idx_to_route_map.get(selected_idx, "/")
        print(f"DEBUG_NAVRAIL_CLICK: NavRail on_change fired! Index: {selected_idx}, Route: '{route}'")
        page.go(route)

    nav_rail = ft.NavigationRail(
        selected_index=0, label_type=ft.NavigationRailLabelType.ALL,
        min_width=100, min_extended_width=250, group_alignment=-0.9,
        destinations=[
            ft.NavigationRailDestination(icon="home_outlined", selected_icon="home", label="Home"),
            ft.NavigationRailDestination(icon="create_new_folder_outlined", selected_icon="create_new_folder", label="Pipeline Wizard"),
            ft.NavigationRailDestination(icon="table_chart_outlined", selected_icon="table_chart", label="View DB"),
            ft.NavigationRailDestination(icon="model_training_outlined", selected_icon="model_training", label="AI Analysis"),
            ft.NavigationRailDestination(icon="settings_outlined", selected_icon="settings", label="Settings"),
        ],
        on_change=nav_rail_on_change,
        expand=False  # Keep this as it fixed clicks
    )
    nav_rail.disabled = False
    print(f"DEBUG_NAVRAIL_SETUP: nav_rail.on_change assigned. nav_rail.disabled={nav_rail.disabled}, nav_rail.expand={nav_rail.expand}")

    main_content_column = ft.Column(
        ref=main_content_area_ref, 
        expand=True, 
        spacing=20,
        scroll=ft.ScrollMode.ADAPTIVE
    )

    # --- Route Handling ---
    def on_route_change(route_event: ft.RouteChangeEvent):
        """Handles route changes and updates the main content area."""
        # page.route will contain the new route string (e.g., "/", "/settings")
        # For parameterized routes (e.g., /item/123), route_event.route might be more specific.
        # For now, we use page.route which is set by page.go()
        current_route = page.route 
        print(f"DEBUG_ROUTE_CHANGE: on_route_change triggered for route: {current_route}")
        
        # Sync NavRail selection if current_route is one of its routes
        for idx, route_str in nav_idx_to_route_map.items():
            if route_str == current_route:
                if nav_rail.selected_index != idx:
                    nav_rail.selected_index = idx
                    nav_rail.update() # Explicitly update NavRail visual state
                    break
        # else: # If route is not in nav_idx_to_route_map (e.g. /database_preview), NavRail selection remains.
            # nav_rail.selected_index = None # Or some other indicator if desired

        if main_content_column:
            main_content_column.controls.clear()
            builder = route_to_view_builder_map.get(current_route)
            print(f"DEBUG_ROUTE_CHANGE: Attempting to build view for route: {current_route} using builder: {builder.__name__ if builder else 'None'}")
            if builder:
                view_content = builder(page) 
                if view_content:
                    main_content_column.controls.append(view_content)
                else:
                    main_content_column.controls.append(ft.Text(f"View for route '{current_route}' returned no content."))
            else:
                # Fallback for unknown routes
                main_content_column.controls.append(ft.Column([
                    ft.Text(f"Error 404: Page not found for route: {current_route}", size=24, weight=ft.FontWeight.BOLD),
                    ft.ElevatedButton("Go to Home", on_click=lambda _: page.go("/"))
                ], horizontal_alignment=ft.CrossAxisAlignment.CENTER))
            main_content_column.update()
        else:
            print("CRITICAL ERROR: main_content_column is None in on_route_change.")
    
    page.on_route_change = on_route_change # Assign the handler

    main_layout_row = ft.Row(
        [
            nav_rail, # Correctly placed here
            ft.VerticalDivider(width=1),
            ft.Container( 
                content=main_content_column,
                expand=True, 
                padding=ft.padding.only(left=20, right=20, bottom=20, top=60), 
            )
        ],
        expand=True, 
        vertical_alignment=ft.CrossAxisAlignment.START 
    )

    page.add(
        ft.Stack(
            [
                main_layout_row, 
                ft.Container( 
                    content=db_display_chip, 
                    right=15, 
                    top=15,
                    padding=ft.padding.all(5), 
                )
            ],
            expand=True
        )
    )

    # --- Initialization ---
    try:
        last_db_path_str = load_last_opened_db()
        if last_db_path_str:
            db_path_to_load = Path(last_db_path_str)
            if db_path_to_load.exists():
                page.active_db_path = db_path_to_load
                update_active_db_display(page.active_db_chip_ref, page.active_db_path)
                print(f"Successfully loaded last opened database: {page.active_db_path}")
            else:
                print(f"Warning: Last opened database file not found: {db_path_to_load}")
                page.active_db_path = None
                save_last_opened_db(None)
                update_active_db_display(page.active_db_chip_ref, None)
        else:
            update_active_db_display(page.active_db_chip_ref, None)
    except Exception as e_init_load:
        print(f"Error during initial database load: {e_init_load}")
        update_active_db_display(page.active_db_chip_ref, None)
    
    # Initial route setup: Navigate to the initial route (e.g., "/")
    # This will trigger on_route_change for the first time.
    page.go("/")
    # page.update() # page.go should trigger necessary updates

if __name__ == "__main__":
    ft.app(
        target=app_main, 
        assets_dir=str(PROJECT_ROOT / "assets") if (PROJECT_ROOT / "assets").is_dir() else None
    ) 