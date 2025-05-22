import sys # Add sys for path manipulation
from pathlib import Path # Add Path for path manipulation

# Add project root to sys.path to allow absolute imports from src
# This assumes database_preview_view.py is in src/views/
PROJECT_ROOT_FOR_PREVIEW = Path(__file__).resolve().parent.parent.parent # Up to project-barney
sys.path.insert(0, str(PROJECT_ROOT_FOR_PREVIEW))

import flet as ft
import sqlite3
# from pathlib import Path # Already imported above

# Assuming db_utils and config are in accessible src locations
# Adjust imports based on your project structure if run directly or as part of main app
try:
    from src.utils.db_utils import fetch_videos_for_view
    from src.config import APP_NAME, ITEMS_PER_PAGE_PREVIEW
except ImportError as e_import:
    print(f"Warning: Could not import from src.utils.db_utils or src.config in database_preview_view.py: {e_import}")
    print(f"Current sys.path: {sys.path}")
    print("Ensure this script is run in an environment where 'src' is discoverable, or adjust sys.path.")
    
    ITEMS_PER_PAGE_PREVIEW = 10 
    
    def fetch_videos_for_view(db_path, page_number, items_per_page):
        print(f"Placeholder: fetch_videos_for_view({db_path}, {page_number}, {items_per_page})")
        return [], 0


# Global state for this view (consider if refs are better if view is rebuilt often)
current_page_num_preview = 1
total_pages_preview = 1
active_db_path_preview = None

data_table_preview_ref = ft.Ref[ft.DataTable]()
current_page_display_ref = ft.Ref[ft.Text]()
pagination_controls_ref = ft.Ref[ft.Row]()
fetch_subtitles_button_ref = ft.Ref[ft.ElevatedButton]()

def update_preview_table_content(page: ft.Page):
    global current_page_num_preview, total_pages_preview, active_db_path_preview

    if not active_db_path_preview or not Path(active_db_path_preview).exists():
        if data_table_preview_ref.current:
            data_table_preview_ref.current.rows = [ft.DataRow(cells=[ft.DataCell(ft.Text("Error: Database path not set or not found.", color="red"))])]
            data_table_preview_ref.current.update()
        if pagination_controls_ref.current:
            for ctrl in pagination_controls_ref.current.controls:
                ctrl.disabled = True
            pagination_controls_ref.current.update()
        if fetch_subtitles_button_ref.current:
            fetch_subtitles_button_ref.current.disabled = True
            fetch_subtitles_button_ref.current.update()
        return

    try:
        videos, total_items = fetch_videos_for_view(active_db_path_preview, current_page_num_preview, ITEMS_PER_PAGE_PREVIEW)
        total_pages_preview = (total_items + ITEMS_PER_PAGE_PREVIEW - 1) // ITEMS_PER_PAGE_PREVIEW
        if total_pages_preview == 0: total_pages_preview = 1 # Ensure at least one page

        rows = []
        if not videos and total_items == 0:
            rows.append(ft.DataRow(cells=[ft.DataCell(ft.Text("No videos found in this database."), col_span=7)]))
        elif not videos and current_page_num_preview > 1: # On a page that no longer exists after deletion/filtering
             rows.append(ft.DataRow(cells=[ft.DataCell(ft.Text(f"No videos on page {current_page_num_preview}. Try page 1."), col_span=7)]))
        else:
            for video in videos:
                rows.append(ft.DataRow(cells=[
                    ft.DataCell(ft.Text(str(video.get('id', 'N/A')))),
                    ft.DataCell(ft.Text(video.get('video_id', 'N/A'))),
                    ft.DataCell(ft.Text(video.get('title', 'N/A'))),
                    ft.DataCell(ft.Text(video.get('published_at', 'N/A'))),
                    ft.DataCell(ft.Text(video.get('status', 'N/A'))),
                    ft.DataCell(ft.Text(video.get('subtitle_status', 'N/A'))),
                    ft.DataCell(ft.Text(video.get('last_updated_at', 'N/A')))
                ]))

        if data_table_preview_ref.current:
            data_table_preview_ref.current.rows = rows
            data_table_preview_ref.current.update()
        
        if current_page_display_ref.current:
            current_page_display_ref.current.value = f"Page {current_page_num_preview} of {total_pages_preview}"
            current_page_display_ref.current.update()

        # Update pagination button states
        if pagination_controls_ref.current:
            prev_button = pagination_controls_ref.current.controls[0] # Assuming order: Prev, Next
            next_button = pagination_controls_ref.current.controls[1]
            prev_button.disabled = (current_page_num_preview == 1)
            next_button.disabled = (current_page_num_preview == total_pages_preview)
            pagination_controls_ref.current.update()
        
        if fetch_subtitles_button_ref.current:
            fetch_subtitles_button_ref.current.disabled = (total_items == 0)
            fetch_subtitles_button_ref.current.update()

    except sqlite3.Error as e:
        if data_table_preview_ref.current:
            data_table_preview_ref.current.rows = [ft.DataRow(cells=[ft.DataCell(ft.Text(f"Database error: {e}", color="red"), col_span=7)])]
            data_table_preview_ref.current.update()
        page.show_snack_bar(ft.SnackBar(ft.Text(f"Error accessing database: {e}"), open=True))
    except Exception as e:
        if data_table_preview_ref.current:
            data_table_preview_ref.current.rows = [ft.DataRow(cells=[ft.DataCell(ft.Text(f"An unexpected error occurred: {e}", color="red"), col_span=7)])]
            data_table_preview_ref.current.update()
        page.show_snack_bar(ft.SnackBar(ft.Text(f"Unexpected error: {e}"), open=True))


def on_page_change_preview(page_obj: ft.Page, direction: int):
    global current_page_num_preview
    new_page = current_page_num_preview + direction
    if 1 <= new_page <= total_pages_preview:
        current_page_num_preview = new_page
        update_preview_table_content(page_obj)

# Placeholder for fetch subtitles functionality
def fetch_subtitles_action(e: ft.ControlEvent):
    db_path = e.page.newly_created_db_path # or active_db_path_preview
    if db_path:
        print(f"Action: Fetch subtitles for database: {db_path}")
        e.page.show_snack_bar(ft.SnackBar(ft.Text(f"Subtitles fetch process would start for: {Path(db_path).name}"), open=True))
        # Here you would trigger the fetch_subtitles.py script
        # For now, let's disable the button to prevent multiple clicks if it were a real process
        if fetch_subtitles_button_ref.current:
            fetch_subtitles_button_ref.current.disabled = True
            fetch_subtitles_button_ref.current.text = "Fetching (Placeholder)..."
            fetch_subtitles_button_ref.current.update()
    else:
        e.page.show_snack_bar(ft.SnackBar(ft.Text("Error: No database path found to fetch subtitles."), open=True))


def build_database_preview_view(page: ft.Page) -> ft.Column:
    global active_db_path_preview, current_page_num_preview
    
    # Retrieve the database path set by the previous view (pipeline_wizard_view)
    active_db_path_preview = getattr(page, 'newly_created_db_path', None)
    current_page_num_preview = 1 # Reset to page 1 when view is built

    view_title = "Database Preview"
    if active_db_path_preview:
        view_title = f"Preview: {Path(active_db_path_preview).name}"
    
    title_text = ft.Text(view_title, size=24, weight=ft.FontWeight.BOLD)

    # Data Table
    data_table = ft.DataTable(
        ref=data_table_preview_ref,
        columns=[
            ft.DataColumn(ft.Text("ID")),
            ft.DataColumn(ft.Text("Video ID")),
            ft.DataColumn(ft.Text("Title")),
            ft.DataColumn(ft.Text("Published")),
            ft.DataColumn(ft.Text("Status")),
            ft.DataColumn(ft.Text("Sub Status")),
            ft.DataColumn(ft.Text("Last Updated"))
        ],
        rows=[ft.DataRow(
            cells=[
                ft.DataCell(
                    ft.Container(
                        content=ft.Text("Loading..."),
                        alignment=ft.alignment.center
                    )
                )
            ]
        )] # Initial loading state
    )

    # Pagination Controls
    prev_button = ft.ElevatedButton("Previous", icon="navigate_before", on_click=lambda e: on_page_change_preview(page, -1), disabled=True)
    next_button = ft.ElevatedButton("Next", icon="navigate_next", on_click=lambda e: on_page_change_preview(page, 1), disabled=True)
    current_page_display = ft.Text(f"Page {current_page_num_preview} of {total_pages_preview}", ref=current_page_display_ref)
    
    pagination_controls = ft.Row(
        ref=pagination_controls_ref,
        controls=[prev_button, current_page_display, next_button],
        alignment=ft.MainAxisAlignment.CENTER,
        spacing=20
    )

    # Action Buttons
    fetch_subtitles_button = ft.ElevatedButton(
        ref=fetch_subtitles_button_ref,
        text="Fetch Subtitles for this Database",
        icon="subtitles_outlined",
        on_click=fetch_subtitles_action,
        bgcolor="blue700",
        color="white",
        disabled=True # Disabled until data is loaded
    )

    # Navigation Buttons
    back_to_wizard_button = ft.ElevatedButton("Back to Pipeline Wizard", icon="arrow_back", on_click=lambda _: page.go("/pipeline_wizard"))
    back_to_home_button = ft.ElevatedButton("Back to Home", icon="home", on_click=lambda _: page.go("/"))
    
    navigation_row = ft.Row(
        [back_to_wizard_button, back_to_home_button],
        alignment=ft.MainAxisAlignment.SPACE_AROUND
    )

    # Initial data load for the view
    # Call update_preview_table_content when the view is first built
    # This needs to be done carefully if build_database_preview_view is called multiple times.
    # A "first load" flag or ensuring page object is fresh might be needed for more complex apps.
    # For now, direct call might be okay, or trigger via page.on_route_change if more robust state needed.
    
    # Defer the first load slightly using page.run_task to ensure refs are set if needed,
    # or simply call if structure guarantees refs are ready.
    # Let's try direct call for simplicity, assuming refs are available when build_ is called.
    # update_preview_table_content(page) -> This might be too early if refs aren't bound.
    # A common Flet pattern is to have an on_mount or similar event for the view/controls.
    # For now, we will rely on the view being built and then an explicit update if needed,
    # or the pipeline wizard will call an update function after navigating.
    # Let's assume `update_preview_table_content` will be called after navigation by pipeline_wizard or via on_connect type event.
    # However, if the user navigates here directly (e.g. bookmark), we need an initial load.
    # The `pipeline_wizard_view` will set `page.newly_created_db_path` and then `page.go()`.
    # `main.py`'s `_switch_view_internal` will then call this `build_` function.
    # At this point, refs should be fine.
    
    # Let's add a simple mechanism to load when the view is built
    # This is slightly simplified; more complex apps might use page.run_task or refs more carefully.
    if active_db_path_preview:
        # Initial load.
        # If ITEMS_PER_PAGE_PREVIEW or other critical configs failed to load, this view will be partial.
        try:
            from src.config import ITEMS_PER_PAGE_PREVIEW as cfg_items_per_page # Re-check import
        except ImportError:
            cfg_items_per_page = 10 # Fallback
            page.show_snack_bar(ft.SnackBar(ft.Text("Warning: Configuration for items per page not fully loaded."), open=True))

        # Check if db_utils were loaded
        if "fetch_videos_for_view" not in globals():
             page.show_snack_bar(ft.SnackBar(ft.Text("Critical Error: Database utilities not loaded. Preview will not function."), open=True))
             # Disable functionality if core components are missing
             if fetch_subtitles_button_ref.current: fetch_subtitles_button_ref.current.disabled = True
             if pagination_controls_ref.current:
                 for ctrl in pagination_controls_ref.current.controls: ctrl.disabled = True


        # Schedule the update after the UI elements are composed.
        # This ensures refs are valid.
        page.run_thread(update_preview_table_content, page)


    elif not active_db_path_preview and hasattr(page, 'newly_created_db_path') and page.newly_created_db_path is None:
        # This case means newly_created_db_path was explicitly set to None or not set from wizard
        title_text.value = "Database Preview (No DB Selected)"
        page.show_snack_bar(ft.SnackBar(ft.Text("No database was created or selected in the previous step."), open=True))
        # Ensure table shows an appropriate message
        if data_table_preview_ref.current:
            data_table_preview_ref.current.rows = [ft.DataRow(cells=[ft.DataCell(ft.Text("Please go back to the Pipeline Wizard and create a database first."), col_span=7)])]
        # Disable buttons
        if fetch_subtitles_button_ref.current: fetch_subtitles_button_ref.current.disabled = True
        if pagination_controls_ref.current:
            for ctrl in pagination_controls_ref.current.controls: ctrl.disabled = True
            if current_page_display_ref.current: current_page_display_ref.current.value = "Page 0 of 0"

    return ft.Column(
        controls=[
            title_text,
            ft.Divider(),
            data_table,
            ft.Divider(),
            pagination_controls,
            ft.Container(height=20), # Spacer
            fetch_subtitles_button,
            ft.Container(height=20), # Spacer
            navigation_row
        ],
        alignment=ft.MainAxisAlignment.START,
        spacing=10,
        expand=True,
        scroll=ft.ScrollMode.ADAPTIVE
    )

# Example of how this view might be tested standalone (requires Flet setup)
if __name__ == "__main__":
    # This example won't fully work without a page object and newly_created_db_path set.
    # It's primarily for syntax checking and basic structure.
    def main(page: ft.Page):
        global fetch_videos_for_view # Declare upfront that we intend to modify the global

        page.title = "Database Preview (Standalone Test)"
        
        # Simulate that a DB path was passed
        # Create a dummy DB for testing
        dummy_db_path = Path("./dummy_preview.db")
        page.newly_created_db_path = str(dummy_db_path)

        if dummy_db_path.exists():
            dummy_db_path.unlink() # Clean up previous dummy
        
        conn = sqlite3.connect(dummy_db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT, video_id TEXT, title TEXT, 
                published_at TEXT, status TEXT, subtitle_status TEXT, last_updated_at TEXT
            )
        """)
        for i in range(25): # Add some dummy data
            cursor.execute("INSERT INTO videos (video_id, title, published_at, status, subtitle_status, last_updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                           (f"vid_dummy_{i+1}", f"Dummy Video Title {i+1}", f"2024-01-{i+1:02d}", "NEW", "pending_check", "2024-01-15"))
        conn.commit()
        conn.close()
        print(f"Created dummy database: {dummy_db_path.resolve()}")

        # Fallback for ITEMS_PER_PAGE_PREVIEW if not imported from config
        if "ITEMS_PER_PAGE_PREVIEW" not in globals():
            ITEMS_PER_PAGE_PREVIEW = 5 # For testing

        # Fallback db_utils if not imported
        # Check if the current global fetch_videos_for_view is the placeholder
        is_placeholder_fetch = False
        if "fetch_videos_for_view" in globals(): # Check if global exists
            _f = globals()["fetch_videos_for_view"]
            if hasattr(_f, "__doc__") and _f.__doc__ and "Placeholder" in _f.__doc__:
                is_placeholder_fetch = True
        else: # Should not happen if defined at module level, but good for robustness
            is_placeholder_fetch = True

        if is_placeholder_fetch:
            print("Using local_fetch_videos for standalone test, replacing placeholder.")
            def local_fetch_videos(db_path_local, page_number_local, items_per_page_local):
                conn_local = sqlite3.connect(db_path_local)
                conn_local.row_factory = sqlite3.Row
                cursor_local = conn_local.cursor()
                offset = (page_number_local - 1) * items_per_page_local
                cursor_local.execute("SELECT COUNT(*) FROM videos")
                total_items_local = cursor_local.fetchone()[0]
                cursor_local.execute("SELECT * FROM videos LIMIT ? OFFSET ?", (items_per_page_local, offset))
                videos_local = [dict(row) for row in cursor_local.fetchall()]
                conn_local.close()
                return videos_local, total_items_local
            
            fetch_videos_for_view = local_fetch_videos # Assigns to the global due to 'global' declaration above
        
        preview_view_content = build_database_preview_view(page)
        page.add(preview_view_content)
        page.update()

    ft.app(target=main) 