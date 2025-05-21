import flet as ft
from pathlib import Path
import sqlite3
import shutil # For creating database directory
import os # For os.getenv
from typing import List, Dict, Optional
from datetime import datetime

# Assuming pipeline_scripts is in the same directory as main.py (src/)
# Adjust if pipeline_scripts is elsewhere relative to the project root when packaging
try:
    from pipeline_scripts.db_info_utils import get_database_summary
    # PROMPTS will be defined directly in this file now
    from pipeline_scripts.batch_ai_analyzer import run_batch_analysis
except ImportError as e:
    # This is a fallback for local development if PYTHONPATH is not set up
    # For packaging, the relative import should work if structure is src/main.py, src/pipeline_scripts/
    print(f"Initial ImportError: {e}") # Log the initial error
    try:
        import sys
        sys.path.append(str(Path(__file__).parent.resolve()))
        from pipeline_scripts.db_info_utils import get_database_summary
        from pipeline_scripts.batch_ai_analyzer import run_batch_analysis
    except ImportError as e_fallback:
        print(f"Fallback ImportError for db_info_utils or batch_ai_analyzer: {e_fallback}")
        # Define a placeholder if import fails, so the app can at least start
        def get_database_summary(db_path):
            return {"video_count": "N/A", "status": "Error loading info"}
        # Fallback for batch_ai_analyzer
        def run_batch_analysis(db_path, api_key, prompt_template_str, prompt_key_for_logging, max_workers, max_videos, progress_callback):
            if progress_callback:
                # Ensure the callback signature matches what update_batch_progress expects
                progress_callback(0, 0, 0, 0, "Error: batch_ai_analyzer.py not found.")
            return {"error": "batch_ai_analyzer.py not found."}

# --- Global Configuration & State ---
APP_NAME = "C-Beam"
PROJECT_ROOT = Path(__file__).parent.parent # Assumes src/main.py, so parent.parent is project root
DATABASES_DIR = PROJECT_ROOT / "databases"
APP_DATA_DIR = PROJECT_ROOT / "app_data"
KNOWN_DATABASES_FILE = APP_DATA_DIR / "known_databases.txt"

# Ensure directories exist
DATABASES_DIR.mkdir(parents=True, exist_ok=True)
APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
if not KNOWN_DATABASES_FILE.exists():
    KNOWN_DATABASES_FILE.touch()

active_db_path_ref = ft.Ref[ft.Text]()
active_db_chip_ref = ft.Ref[ft.Chip]() # Renamed from active_db_name_ref
page_ref = ft.Ref[ft.Page]()
main_content_area_ref = ft.Ref[ft.Column]()
available_databases_column_ref = ft.Ref[ft.Column]()

# --- Refs for View Database Pagination ---
current_db_page_ref = ft.Ref[int]()
total_db_videos_ref = ft.Ref[int]()
db_page_size_ref = ft.Ref[int]() # To store page size, e.g., 10

# GEMINI_API_KEY retrieval for now, or plan to move to settings
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Define PROMPTS directly in main.py
PROMPTS = {
    "summary": """You are an expert linguist and religious content analyst.

TASK
Summarize the following Arabic transcript of a TV show with multiple hosts.

TRANSCRIPT:
{transcript_content}

Please provide a concise English summary of this content in under 200 words. Focus on the main themes, arguments, and significant points made on the show. For any particularly controversial statements, include a timestamp and then a guess at who might be speaking. If it's not clear, don't guess and only include the timestamp. 

Your response should be ONLY the plain text summary with no additional formatting, headings, or explanations.
If the transcript is empty, unclear, or doesn't contain enough content to summarize, simply state that briefly.""",

    "themes": """You are an expert linguist and religious content analyst.

TASK
Exclusively in English without retaining any Arabic terms, fully explain what is said about the following themes in the transcript. If there is nothing about a theme, exclude the bulletpoint. Prioritise cases in which the words are explicitly mentioned, but indirect mentions are okay too.
Themes:Jihad, martyrdom, resistance, conquest, fighters, armed struggle, mujahideen, Hamas, caliphate, unbelievers, apostates, non-Muslims, Westerners, Jews, Zionist, Holocaust, Lobbying.
If one of the themes is present, then always provide a complete explanation of the way in which it is mentioned. EVERY time a quote is picked out, explain the context in which it is being used. If a theme is not present, leave it out. Only if none of the themes are mentioned then say that none of the themes have been mentioned.
Focus mainly on direct mention of these words or themes, although indirect mentions are also okay.


TRANSCRIPT:
{transcript_content}

Your response should be the themes AND their full explanations. Do not include any markdown formatting. Your response should be only in English and any Arabic terms should be translated and explained."""
}

# --- Database Helper Functions ---
def load_known_databases():
    """Loads database paths from known_databases.txt and scans DATABASES_DIR."""
    known_paths = set()
    if KNOWN_DATABASES_FILE.exists():
        with open(KNOWN_DATABASES_FILE, "r") as f:
            for line in f:
                path_str = line.strip()
                if path_str: # Ensure not an empty line
                    known_paths.add(Path(path_str).resolve()) # Store absolute, resolved paths

    # Scan the DATABASES_DIR for .db files
    if DATABASES_DIR.exists():
        for db_file in DATABASES_DIR.glob("*.db"):
            known_paths.add(db_file.resolve())
    
    # Persist the combined list (ensures DATABASES_DIR items are in known_databases.txt)
    # This also deduplicates and resolves paths.
    unique_resolved_paths = sorted(list(known_paths))
    with open(KNOWN_DATABASES_FILE, "w") as f:
        for path in unique_resolved_paths:
            f.write(str(path) + "\n")
            
    return unique_resolved_paths

def add_known_database(db_path: Path, page: ft.Page):
    """Adds a database path to known_databases.txt and updates the view."""
    db_path = db_path.resolve() # Ensure absolute path
    current_paths = load_known_databases()
    if db_path not in current_paths:
        with open(KNOWN_DATABASES_FILE, "a") as f:
            f.write(str(db_path) + "\n")
    if page and hasattr(page, 'selected_db_changed_callback'): # Check if callback exists
        page.selected_db_changed_callback() # Trigger update in Change DB view

def update_active_db_display(db_path: Path | None):
    """Updates the database display Chip."""
    if active_db_chip_ref.current: 
        db_chip = active_db_chip_ref.current
        # Instead of None, use an empty Text control for the label
        if not isinstance(db_chip.label, ft.Text) or db_chip.label.value != "":
            db_chip.label = ft.Text("") 

        if db_path: 
            db_display_name = db_path.stem 
            db_chip.leading = ft.Icon(name="storage", color="primary", size=18) # Slightly larger icon
            db_chip.tooltip = f"Active database: {db_display_name}"
        else: # No DB Path
            db_chip.leading = ft.Row(
                [
                    ft.Icon(name="storage_outlined", opacity=0.7, size=18), # Slightly larger icon
                    ft.Icon(name="cancel_outlined", color="error", size=18, tooltip="No database selected") # Slightly larger icon
                ], 
                spacing=4, 
                vertical_alignment=ft.CrossAxisAlignment.CENTER
            )
            db_chip.tooltip = "No database active"
        
        db_chip.visible = True 
        db_chip.update()

def open_database(db_path_str: str, page: ft.Page):
    db_path = Path(db_path_str)
    if page_ref.current and hasattr(page_ref.current, 'active_db_path'):
        page_ref.current.active_db_path = db_path
        update_active_db_display(db_path)
        
        page.snack_bar = ft.SnackBar(ft.Text(f"Opened database: {db_path.name}"), open=True)
        page.update()
    else:
        print(f"Error: page_ref.current not set or missing active_db_path attribute.")
        page.snack_bar = ft.SnackBar(ft.Text(f"Error opening {db_path.name}"), open=True, bgcolor="errorcontainer")
        page.update()

# --- Helper to check for active DB ---
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

# --- Placeholder for fetching video data for View Database page ---
def fetch_videos_for_view(db_path_str: str, search_term: Optional[str] = None, page_number: int = 1, page_size: int = 10) -> Dict:
    """ Fetches paginated video data from the database. """
    videos = []
    total_count = 0
    conn = None
    try:
        conn = sqlite3.connect(db_path_str)
        conn.row_factory = sqlite3.Row # Access columns by name
        cursor = conn.cursor()

        # Define columns for the DataTable view + details dialog
        columns_to_select = [
            "id", "video_id", "title", "channel_id", "published_at", "status",
            "subtitle_status", "download_status", "transcription_status",
            "segmentation_10w_status", "analysis_status", "text_source",
            "ai_analysis_content", # Keep for details dialog
            "last_updated_at"
        ]
        select_cols_str = ', '.join(columns_to_select)

        base_query = f"FROM videos"
        count_query_str = f"SELECT COUNT(*) as total_count {base_query}"
        
        params = []
        where_clauses = []

        if search_term:
            # Simple search across title, video_id, channel_id
            where_clauses.append("(title LIKE ? OR video_id LIKE ? OR channel_id LIKE ?)")
            params.extend([f"%{search_term}%", f"%{search_term}%", f"%{search_term}%"])
        
        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)
            count_query_str = f"SELECT COUNT(*) as total_count {base_query}" # Rebuild count_query with WHERE

        # Get total count first
        cursor.execute(count_query_str, params) # Params are for the WHERE clause if present
        count_row = cursor.fetchone()
        if count_row:
            total_count = count_row['total_count']

        # Now fetch the paginated data
        offset = (page_number - 1) * page_size
        paginated_query_str = f"SELECT {select_cols_str} {base_query} ORDER BY last_updated_at DESC LIMIT ? OFFSET ?"
        
        # Parameters for paginated query: search terms (if any) + limit + offset
        paginated_params = params + [page_size, offset]
        
        cursor.execute(paginated_query_str, paginated_params)
        rows = cursor.fetchall()

        for row in rows:
            videos.append(dict(row))
        
        print(f"Fetched {len(videos)} videos (page {page_number}/{ (total_count + page_size -1) // page_size if page_size > 0 else 1}) from {db_path_str} with search: '{search_term if search_term else 'N/A'}'")

    except sqlite3.Error as e:
        print(f"Database error in fetch_videos_for_view: {e}")
        # Potentially show this error in the UI
    except Exception as e:
        print(f"General error in fetch_videos_for_view: {e}")
    finally:
        if conn:
            conn.close()
    
    return {"videos": videos, "total_count": total_count}

# --- Placeholder for showing video details dialog ---
def show_video_details_dialog(page: ft.Page, video_data: Dict):
    """ Placeholder: Shows a dialog with AI analysis and transcript. """
    
    transcript_content_text = "Could not load transcript."
    # In a real implementation, load transcript from video_data['source_text_path']
    # For example:
    # try:
    #     with open(video_data.get('source_text_path', ''), 'r', encoding='utf-8') as f:
    #         transcript_content_text = f.read()
    # except Exception as e:
    #     transcript_content_text = f"Error loading transcript: {e}"
    # For this placeholder, we'll use a fixed string
    if video_data.get('source_text_path'): # simulate having a path
        transcript_content_text = "This is a placeholder for the full transcript content that would be scrollable. Timestamps would appear here if available in the source file."
    else:
        # This case will now be hit more often if source_text_path is not in the DB
        transcript_content_text = "Transcript path not available or column removed from query."

    dialog_content = ft.Column(
        [
            ft.Text(f"Details for: {video_data.get('title', 'N/A')}", style=ft.TextThemeStyle.HEADLINE_SMALL, weight=ft.FontWeight.BOLD),
            ft.Text("AI Analysis:", style=ft.TextThemeStyle.TITLE_MEDIUM),
            ft.Container(
                ft.Text(video_data.get('ai_analysis_content', 'No AI analysis available.'), selectable=True),
                border=ft.border.all(1, "grey"), padding=10, border_radius=5, margin=ft.margin.only(bottom=10)
            ),
            ft.Text("Transcript:", style=ft.TextThemeStyle.TITLE_MEDIUM),
            ft.Container(
                ft.Text(transcript_content_text, selectable=True),
                border=ft.border.all(1, "grey"), padding=10, border_radius=5, 
                height=200, # Make transcript area scrollable
                scroll=ft.ScrollMode.ADAPTIVE
            ),
        ],
        tight=True, # Make column take minimum space needed by its content
        width=600, # Define a width for the dialog content
        scroll=ft.ScrollMode.ADAPTIVE # Allow overall dialog content to scroll if very long
    )

    dialog = ft.AlertDialog(
        modal=True,
        title=ft.Text("Video Details"),
        content=dialog_content,
        actions=[
            ft.TextButton("Close", on_click=lambda _: close_dialog(page, dialog)),
        ],
        actions_alignment=ft.MainAxisAlignment.END,
    )
    page.dialog = dialog
    dialog.open = True
    page.update()

def close_dialog(page, dialog_instance):
    dialog_instance.open = False
    page.update()

# --- View Builder Functions ---

def build_home_view(page: ft.Page):
    features = [
        ft.Container(
            content=ft.Column([
                ft.Icon("search", size=50),
                ft.Text("Channel Discovery", theme_style=ft.TextThemeStyle.HEADLINE_SMALL),
                ft.Text("Find YouTube channels by keywords.")
            ]),
            alignment=ft.alignment.center,
            padding=20,
            border_radius=10,
            width=200, # Added fixed width for items in a Row
            # ink=True # Removed for now, check flet version for compatibility
        ),
        ft.Container(
            content=ft.Column([
                ft.Icon("subtitles", size=50),
                ft.Text("Subtitle Processing", theme_style=ft.TextThemeStyle.HEADLINE_SMALL),
                ft.Text("Grab, fix, and translate subtitles.")
            ]),
            alignment=ft.alignment.center,
            padding=20,
            border_radius=10,
            width=200, # Added fixed width
        ),
        ft.Container(
            content=ft.Column([
                ft.Icon("model_training", size=50),
                ft.Text("AI Analysis", theme_style=ft.TextThemeStyle.HEADLINE_SMALL),
                ft.Text("Summarize content, identify themes.")
            ]),
            alignment=ft.alignment.center,
            padding=20,
            border_radius=10,
            width=200, # Added fixed width
        ),
         ft.Container(
            content=ft.Column([
                ft.Icon("table_chart", size=50),
                ft.Text("Data Export", theme_style=ft.TextThemeStyle.HEADLINE_SMALL),
                ft.Text("Export your findings to CSV.")
            ]),
            alignment=ft.alignment.center,
            padding=20,
            border_radius=10,
            width=200, # Added fixed width
        )
    ]

    # Replaced Carousel with a horizontally scrollable Row
    feature_display = ft.Row(
        controls=features,
        scroll=ft.ScrollMode.AUTO, # Enable horizontal scrolling
        spacing=20,
        vertical_alignment=ft.CrossAxisAlignment.START # Align items to the top of the row
    )

    return ft.Column(
        [
            ft.Text(APP_NAME, theme_style=ft.TextThemeStyle.DISPLAY_MEDIUM, weight=ft.FontWeight.BOLD),
            ft.Text(
                "I've watched C-beams glitter in the dark, near the Tannhauser gate.",
                
                theme_style=ft.TextThemeStyle.HEADLINE_SMALL,
            ),
            ft.Divider(height=20, color="transparent"),
            ft.Text("Key Features:", theme_style=ft.TextThemeStyle.TITLE_LARGE),
            feature_display, # Use the new Row here
            ft.Divider(height=20, color="transparent"),
            ft.Text("Get started by creating or opening a database from the sidebar.", theme_style=ft.TextThemeStyle.BODY_LARGE)
        ],
        alignment=ft.MainAxisAlignment.START,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        spacing=15,
        # expand=True, # Let content scroll if it overflows
        scroll=ft.ScrollMode.ADAPTIVE,
    )

def build_create_new_db_view(page: ft.Page):
    db_name_field = ft.TextField(label="New Database Name (e.g., 'tech_channels')", width=300)

    def create_db_action(e):
        db_name_raw = db_name_field.value
        if not db_name_raw:
            page.snack_bar = ft.SnackBar(ft.Text("Database name cannot be empty."), open=True, bgcolor="errorcontainer")
            page.update()
            return

        # Sanitize name (simple sanitization)
        db_name = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in db_name_raw)
        if not db_name: # if sanitization results in empty string
             page.snack_bar = ft.SnackBar(ft.Text("Invalid characters in database name."), open=True, bgcolor="errorcontainer")
             page.update()
             return

        if not db_name.endswith(".db"):
            db_name += ".db"
        
        new_db_path = DATABASES_DIR / db_name

        if new_db_path.exists():
            page.snack_bar = ft.SnackBar(ft.Text(f"Database '{db_name}' already exists."), open=True, bgcolor="warningcontainer")
            page.update()
            return

        try:
            conn = sqlite3.connect(new_db_path)
            conn.close()
            
            add_known_database(new_db_path, page)
            open_database(str(new_db_path), page) 
            page.snack_bar = ft.SnackBar(ft.Text(f"Database '{db_name}' created and opened."), open=True)
            page.update()
            db_name_field.value = "" 
            db_name_field.update()
            if hasattr(page, 'switch_view_callback'):
                page.switch_view_callback(0) 

        except Exception as ex:
            page.snack_bar = ft.SnackBar(ft.Text(f"Error creating database: {ex}"), open=True, bgcolor="errorcontainer")
            page.update()

    return ft.Column(
        [
            ft.Text("Create New Database", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM),
            ft.Text(f"Databases will be saved in: {DATABASES_DIR.resolve()}"),
            db_name_field,
            ft.ElevatedButton("Create Database", icon="create_new_folder", on_click=create_db_action)
        ],
        spacing=20,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER
    )

def build_change_database_view(page: ft.Page):
    print("DEBUG: Building Change Database view...")

    # Core logic to update the database list display
    def _update_db_list_contents(target_column: ft.Column):
        print(f"DEBUG: _update_db_list_contents called for column: {target_column}")
        if not target_column:
            print("Error: target_column is None in _update_db_list_contents")
            return
        
        target_column.controls.clear()
        db_paths = load_known_databases()

        if not db_paths:
            target_column.controls.append(ft.Text("No databases found. Create one or add an existing one."))
        else:
            for db_path in db_paths:
                summary = get_database_summary(str(db_path))
                card = ft.Card(
                    content=ft.Container(
                        content=ft.Column(
                            [
                                ft.Text(db_path.name, weight=ft.FontWeight.BOLD),
                                ft.Text(f"Location: {db_path.parent}", size=10, italic=True),
                                ft.Text(f"Videos: {summary.get('video_count', 'N/A')}"),
                                ft.Text(f"Status: {summary.get('status', 'N/A')}"),
                                ft.ElevatedButton(
                                    "Open Database",
                                    icon="input",
                                    on_click=lambda _, path_str=str(db_path): open_database(path_str, page),
                                    width=200
                                )
                            ],
                            spacing=5
                        ),
                        width=400,
                        padding=10,
                        border_radius=5
                    )
                )
                target_column.controls.append(card)
        target_column.update()

    # This is the column we want to populate, with its ref
    available_databases_column = ft.Column(
        ref=available_databases_column_ref, 
        spacing=10, 
        scroll=ft.ScrollMode.ADAPTIVE
    )
    
    # Renamed trigger function
    def local_trigger_db_list_refresh(): 
        print("DEBUG: local_trigger_db_list_refresh called.")
        col_instance = available_databases_column_ref.current
        if col_instance:
            _update_db_list_contents(col_instance)
        else:
            print("DEBUG: local_trigger_db_list_refresh: available_databases_column_ref.current is None")

    # Callback for when a DB is added elsewhere (e.g., create new)
    page.selected_db_changed_callback = local_trigger_db_list_refresh
    
    # Store a reference to this view's refresh function on the page, so switch_view can call it.
    if not hasattr(page, 'view_refresh_triggers'):
        page.view_refresh_triggers = {}
    page.view_refresh_triggers[2] = local_trigger_db_list_refresh # 2 is the index for Change DB view

    def add_existing_db_dialog(e):
        def on_dialog_result(e_picker: ft.FilePickerResultEvent):
            if e_picker.files and len(e_picker.files) > 0:
                selected_path = Path(e_picker.files[0].path)
                if selected_path.suffix == ".db":
                    add_known_database(selected_path, page) 
                    page.snack_bar = ft.SnackBar(ft.Text(f"Added {selected_path.name} to known databases."), open=True)
                    page.update()
                else:
                    page.snack_bar = ft.SnackBar(ft.Text("Please select a valid .db file."), open=True, bgcolor="errorcontainer")
                    page.update()
            page.update() 

        file_picker = ft.FilePicker(on_result=on_dialog_result)
        page.overlay.append(file_picker)
        page.update()
        file_picker.pick_files(
            dialog_title="Select Database File",
            allow_multiple=False,
            allowed_extensions=["db"]
        )
    
    return ft.Column(
        [
            ft.Row([
                ft.Text("Manage Databases", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM),
                ft.IconButton("refresh", on_click=lambda e_click: local_trigger_db_list_refresh(), tooltip="Refresh List"),
                ft.ElevatedButton("Add Existing DB File", icon="add_circle_outline", on_click=add_existing_db_dialog)
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            ft.Text("Select a database to open, or add an existing one to your list."),
            ft.Divider(),
            available_databases_column 
        ],
        expand=True,
        spacing=10
    )

def build_view_database_view(page: ft.Page):
    if not check_active_db_and_show_snackbar(page):
        return ft.Column([ft.Text("Please select a database to view its content.", theme_style=ft.TextThemeStyle.TITLE_LARGE, text_align=ft.TextAlign.CENTER)], horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.CENTER, expand=True)

    search_field = ft.TextField(label="Search by title, video ID, or channel ID...", hint_text="Enter keywords and press Enter or click Search", width=500, on_submit=lambda e: perform_search(e, is_new_search=True))
    
    # Define DataTable columns for the simplified view
    datatable_columns = [
        # ft.DataColumn(ft.Text("ID"), numeric=True), # No longer shown directly
        ft.DataColumn(ft.Text("Video ID (Clickable)")),
        ft.DataColumn(ft.Text("Title")),
        # ft.DataColumn(ft.Text("Channel ID")), # No longer shown directly
        ft.DataColumn(ft.Text("Published")), 
        # ft.DataColumn(ft.Text("Status")), # No longer shown directly
        # ft.DataColumn(ft.Text("Subtitles")), # No longer shown directly
        # ft.DataColumn(ft.Text("DL Status")), # No longer shown directly
        # ft.DataColumn(ft.Text("Transcript")), # No longer shown directly
        # ft.DataColumn(ft.Text("Segment")), # No longer shown directly
        # ft.DataColumn(ft.Text("AI Status")), # No longer shown directly
        # ft.DataColumn(ft.Text("Text Src")), # No longer shown directly
        # ft.DataColumn(ft.Text("Updated")), # No longer shown directly
        ft.DataColumn(ft.Text("Actions")),
    ]

    data_table = ft.DataTable(
        columns=datatable_columns,
        rows=[],
        column_spacing=10,
        divider_thickness=0.5,
        # heading_row_color=ft.colors.SURFACE_VARIANT, # Example styling
        # border=ft.border.all(1, ft.colors.OUTLINE), # Example styling
        # border_radius=5, # Example styling
        expand=True, # Allow table to expand within its container
    )
    
    # Using a Ref for data_table to update its rows
    data_table_ref = ft.Ref[ft.DataTable]()
    data_table_ref.current = data_table

    def update_pagination_controls():
        if not pagination_controls_ref.current or total_db_videos_ref.current is None or current_db_page_ref.current is None or db_page_size_ref.current is None:
            if pagination_controls_ref.current: pagination_controls_ref.current.visible = False
            if pagination_controls_ref.current: pagination_controls_ref.current.update()
            return

        total_pages = (total_db_videos_ref.current + db_page_size_ref.current - 1) // db_page_size_ref.current if db_page_size_ref.current > 0 else 1
        total_pages = max(1, total_pages) # Ensure at least 1 page

        page_info_text.value = f"Page {current_db_page_ref.current} of {total_pages}"
        prev_button.disabled = current_db_page_ref.current <= 1
        next_button.disabled = current_db_page_ref.current >= total_pages
        
        pagination_controls_ref.current.visible = total_db_videos_ref.current > 0 # Show if there are any videos
        pagination_controls_ref.current.update()
        page_info_text.update()
        prev_button.update()
        next_button.update()

    def update_data_table(data: Dict): # Expects dict {videos: [], total_count: X}
        videos = data.get("videos", [])
        total_count = data.get("total_count", 0)

        if current_db_page_ref.current is None: current_db_page_ref.current = 1 # Should be set by caller
        if db_page_size_ref.current is None: db_page_size_ref.current = 10 # Default

        total_db_videos_ref.current = total_count
        
        if data_table_ref.current:
            data_table_ref.current.rows.clear()
            if not videos:
                # Optional: Display a message if no videos on the current page/search
                # For now, an empty table means no videos for the current view.
                pass
            else:
                for video_data in videos:
                    video_id = video_data.get('video_id', 'N/A')
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                    
                    cells = [
                        # ft.DataCell(ft.Text(str(video_data.get('id', 'N/A')))), # Removed
                        ft.DataCell(
                            ft.Text(video_id, color="lightblue", overflow=ft.TextOverflow.ELLIPSIS), 
                            on_tap=lambda e, url=video_url: page.launch_url(url) # Corrected: e is passed by on_tap
                        ),
                        ft.DataCell(ft.Text(video_data.get('title', 'N/A'), overflow=ft.TextOverflow.ELLIPSIS)),
                        # ft.DataCell(ft.Text(video_data.get('channel_id', 'N/A'))), # Removed
                        ft.DataCell(ft.Text(str(video_data.get('published_at', 'N/A')).split(' ')[0] if video_data.get('published_at') else 'N/A')),
                        # ... other cells removed for brevity ...
                        ft.DataCell(ft.IconButton(icon="visibility", tooltip="View Details", on_click=lambda _, vd=video_data: show_video_details_dialog(page, vd)))
                    ]
                    data_table_ref.current.rows.append(ft.DataRow(cells=cells))
            data_table_ref.current.update()
        
        update_pagination_controls()

    def _fetch_and_update_page_data(page_num_to_load: int, search_term_val: Optional[str]):
        if page.active_db_path and current_db_page_ref.current is not None and db_page_size_ref.current is not None:
            current_db_page_ref.current = page_num_to_load # Update current page before fetching
            fetched_data = fetch_videos_for_view(
                str(page.active_db_path), 
                search_term_val,
                page_number=current_db_page_ref.current,
                page_size=db_page_size_ref.current
            )
            update_data_table(fetched_data)
        else:
            update_data_table({"videos": [], "total_count": 0}) # Clear table and hide pagination

    def perform_search(e=None, is_new_search: bool = False):
        if is_new_search and current_db_page_ref.current is not None:
            current_db_page_ref.current = 1 # Reset to page 1 for new search
        
        search_term = search_field.value
        _fetch_and_update_page_data(current_db_page_ref.current or 1, search_term)

    def initial_load():
        if current_db_page_ref.current is None: current_db_page_ref.current = 1
        if db_page_size_ref.current is None: db_page_size_ref.current = 10
        search_term = search_field.value # Consider if search should persist or clear on view load
        _fetch_and_update_page_data(1, search_term) # Load page 1

    def go_to_next_page(page_instance: ft.Page, current_search_term: Optional[str]):
        if total_db_videos_ref.current is not None and current_db_page_ref.current is not None and db_page_size_ref.current is not None:
            total_pages = (total_db_videos_ref.current + db_page_size_ref.current - 1) // db_page_size_ref.current
            if current_db_page_ref.current < total_pages:
                _fetch_and_update_page_data(current_db_page_ref.current + 1, current_search_term)

    def go_to_prev_page(page_instance: ft.Page, current_search_term: Optional[str]):
        if current_db_page_ref.current is not None and current_db_page_ref.current > 1:
            _fetch_and_update_page_data(current_db_page_ref.current - 1, current_search_term)
            
    # Ensure page specific refresh trigger is set up
    if not hasattr(page, 'view_refresh_triggers'):
        page.view_refresh_triggers = {}
    page.view_refresh_triggers[3] = initial_load

    # --- Pagination Controls ---
    prev_button = ft.IconButton(icon="arrow_back", on_click=lambda e: go_to_prev_page(page, search_field.value), disabled=True)
    next_button = ft.IconButton(icon="arrow_forward", on_click=lambda e: go_to_next_page(page, search_field.value), disabled=True)
    page_info_text = ft.Text("Page 1 of 1")
    pagination_controls_ref = ft.Ref[ft.Row]() # Ref for the Row containing pagination controls

    pagination_row = ft.Row(
        [prev_button, page_info_text, next_button],
        alignment=ft.MainAxisAlignment.CENTER,
        ref=pagination_controls_ref,
        visible=False # Initially hidden until data is loaded
    )
    
    # Initialize pagination state Refs
    if current_db_page_ref.current is None: current_db_page_ref.current = 1
    if total_db_videos_ref.current is None: total_db_videos_ref.current = 0
    if db_page_size_ref.current is None: db_page_size_ref.current = 10 # Default page size

    # Container for the DataTable to allow horizontal scrolling if needed
    table_container = ft.Row(
        [data_table],
        scroll=ft.ScrollMode.ADAPTIVE, 
        expand=True
    )

    return ft.Column(
        [
            ft.Text("View Database Content", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM),
            ft.Row([search_field, ft.ElevatedButton("Search", icon="search", on_click=perform_search)], alignment=ft.MainAxisAlignment.START),
            ft.Divider(),
            table_container, # Add the scrollable container for the table
            pagination_row # Add pagination controls here
        ],
        expand=True, spacing=10, 
        horizontal_alignment=ft.CrossAxisAlignment.STRETCH # Ensure column stretches
    )

def build_run_ai_analysis_view(page: ft.Page):
    prompt_type_radio_group = ft.RadioGroup(
        content=ft.Row(
            controls=[
                ft.Radio(value="summary", label="Summary of Pending Videos"),
                ft.Radio(value="themes", label="Thematic Analysis of Pending Videos"),
            ]
        ),
        value="summary",
    )

    batch_status_text = ft.Text("Batch analysis status will appear here.", selectable=True)
    progress_bar = ft.ProgressBar(value=0, visible=False, width=page.width*0.8 if page.width else 500) # Adjust width

    # Use a Ref for the button to disable/enable it
    run_analysis_button_ref = ft.Ref[ft.ElevatedButton]()

    def update_batch_progress(processed, total, success, failed, final_message=None):
        if total > 0:
            progress_bar.value = processed / total
        else:
            progress_bar.value = 0
        
        status_msg = f"Processed: {processed}/{total} | Successful: {success} | Failed: {failed}"
        if final_message:
            status_msg = f"{final_message} | {status_msg}"

        batch_status_text.value = status_msg
        
        # Ensure UI updates happen on the Flet thread
        if page_ref.current: # Check if page context is available
            batch_status_text.update() # Update specific controls
            progress_bar.update()
            page_ref.current.update() # Broader page update if necessary for layout changes
        else: # Fallback if no page context (e.g. thread ended after page closed)
            print(f"Debug Progress (no page context): {status_msg}")

    def batch_analysis_thread_worker(db_path_str, api_key_str, selected_prompt_key):
        prompt_template = PROMPTS.get(selected_prompt_key)
        if not prompt_template:
            # This update needs to be thread-safe to the UI
            page.call_soon_threadsafe(update_batch_progress, 0,0,0,0, f"Error: Prompt '{selected_prompt_key}' not found.")
            if run_analysis_button_ref.current:
                 page.call_soon_threadsafe(setattr, run_analysis_button_ref.current, 'disabled', False)
                 page.call_soon_threadsafe(run_analysis_button_ref.current.update)
            return

        def progress_callback_for_batch(processed, total, success, failed, message_override=None):
            # This callback is called from the worker thread, so schedule UI updates on Flet's main thread.
            final_msg_to_pass = message_override
            page.call_soon_threadsafe(update_batch_progress, processed, total, success, failed, final_msg_to_pass)

        try:
            results = run_batch_analysis(
                db_path=db_path_str,
                api_key=api_key_str,
                prompt_template_str=prompt_template,
                prompt_key_for_logging=selected_prompt_key,
                max_workers=4, # Or make this configurable
                max_videos=None, # Process all pending unless specified
                progress_callback=progress_callback_for_batch
            )
            final_summary = f"Batch ({selected_prompt_key}) complete."
            if results.get("error"):
                final_summary = f"Batch ({selected_prompt_key}) failed: {results.get('error')}"
            page.call_soon_threadsafe(update_batch_progress, results.get('total_processed',0), results.get('total_processed',0), results.get('successful',0), results.get('failed',0), final_summary)
        except Exception as ex:
            error_msg = f"Critical error running batch analysis: {ex}"
            page.call_soon_threadsafe(update_batch_progress, 0,0,0,0, error_msg)
            print(f"Batch Analysis Thread Error: {ex}") # Also log to console
        finally:
            # Re-enable the button
            if run_analysis_button_ref.current:
                 page.call_soon_threadsafe(setattr, run_analysis_button_ref.current, 'disabled', False)
                 page.call_soon_threadsafe(run_analysis_button_ref.current.update)
            progress_bar.visible = False # Hide progress bar after completion/error
            page.call_soon_threadsafe(progress_bar.update)

    def run_analysis_click(e):
        if not check_active_db_and_show_snackbar(page): # Use the helper
            return

        current_api_key = GEMINI_API_KEY # Using global, consider getting from settings later
        if not current_api_key:
            page.snack_bar = ft.SnackBar(ft.Text("GEMINI_API_KEY not found. Configure in .env or settings."), open=True, bgcolor="errorcontainer")
            page.update()
            return

        selected_prompt_key = prompt_type_radio_group.value
        db_path_to_use = str(page.active_db_path)

        batch_status_text.value = f"Starting batch analysis ('{selected_prompt_key}') on {page.active_db_path.name}..."
        progress_bar.value = 0
        progress_bar.visible = True
        batch_status_text.update()
        progress_bar.update()
        if run_analysis_button_ref.current: run_analysis_button_ref.current.disabled = True
        page.update()

        # Run the batch analysis in a separate thread
        page.run_thread_async(
            batch_analysis_thread_worker, 
            db_path_to_use, 
            current_api_key, 
            selected_prompt_key
        )

    run_analysis_button = ft.ElevatedButton(
        ref=run_analysis_button_ref,
        text="Start Batch AI Analysis on Pending Videos", 
        icon="play_circle_filled", 
        on_click=run_analysis_click
    )

    return ft.Column(
        [
            ft.Text("Run Batch AI Analysis", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM),
            ft.Text("Select an analysis type to run on all pending videos in the active database."),
            ft.Divider(),
            ft.Text("Select Analysis Type:", weight=ft.FontWeight.BOLD),
            prompt_type_radio_group,
            ft.Row([run_analysis_button], alignment=ft.MainAxisAlignment.START),
            ft.Divider(height=10, color="transparent"),
            ft.Text("Batch Progress:", weight=ft.FontWeight.BOLD),
            progress_bar,
            batch_status_text,
        ],
        spacing=15,
        scroll=ft.ScrollMode.ADAPTIVE,
    )

def build_settings_view(page: ft.Page):
    # Placeholder for settings
    # Example: Gemini API Key input
    # gemini_key_field = ft.TextField(label="Gemini API Key", password=True, can_reveal_password=True)
    # save_button = ft.ElevatedButton("Save Settings")
    # return ft.Column([
    #     ft.Text("Application Settings", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM),
    #     gemini_key_field,
    #     save_button
    # ])
    return ft.Column(
        [
            ft.Text("Application Settings", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM),
            ft.Text("Settings controls will be here (e.g., API keys, theme preferences).")
        ],
        spacing=10
    )

# --- Main Application ---
def main(page: ft.Page):
    page.title = APP_NAME
    page.theme_mode = ft.ThemeMode.DARK
    page.vertical_alignment = ft.MainAxisAlignment.START 
    page.horizontal_alignment = ft.CrossAxisAlignment.START 
    page_ref.current = page 
    page.active_db_path = None 
    page.window_icon = "assets/logo.png" 

    page.appbar = None 

    # Database Display Chip - will be positioned in a Stack later
    db_display_chip = ft.Chip(
        ref=active_db_chip_ref,
        label=ft.Text(""), # Use an empty Text control instead of None
        # leading content set by update_active_db_display
        visible=True, # Chip is always visible
        # tooltip set by update_active_db_display
    )

    # Navigation Rail (Sidebar)
    nav_rail = ft.NavigationRail(
        selected_index=0,
        label_type=ft.NavigationRailLabelType.ALL,
        min_width=100, # Collapsed width
        min_extended_width=250, # Extended width (if we enable extension)
        group_alignment=-0.9, 
        destinations=[
            ft.NavigationRailDestination(icon="home_outlined", selected_icon="home", label="Home"),
            ft.NavigationRailDestination(icon="create_new_folder_outlined", selected_icon="create_new_folder", label="New DB"),
            ft.NavigationRailDestination(icon="swap_horiz_outlined", selected_icon="swap_horiz", label="Change DB"),
            ft.NavigationRailDestination(icon="table_chart_outlined", selected_icon="table_chart", label="View DB"),
            ft.NavigationRailDestination(icon="model_training_outlined", selected_icon="model_training", label="AI Analysis"),
            ft.NavigationRailDestination(icon="settings_outlined", selected_icon="settings", label="Settings"),
        ],
        trailing=None # Removed chip from here
    )

    # Main content area (center part of the page)
    main_content_column = ft.Column(ref=main_content_area_ref, expand=True, spacing=20, scroll=ft.ScrollMode.ADAPTIVE)

    # --- View Switching Logic (remains the same) ---
    view_builders = {
        0: build_home_view,
        1: build_create_new_db_view,
        2: build_change_database_view,
        3: build_view_database_view,
        4: build_run_ai_analysis_view,
        5: build_settings_view,
    }

    def switch_view(selected_idx: int, current_page: ft.Page):
        main_content_column.controls.clear()
        builder = view_builders.get(selected_idx)
        if builder:
            view_content = builder(current_page) # Get the content first
            main_content_column.controls.append(view_content)
        else:
            main_content_column.controls.append(ft.Text(f"View {selected_idx} not found."))
        main_content_column.update() # Update the page with the new view structure

        # After adding the view, check if it has a specific refresh/load function to call
        # This was previously only for index 2, now generalized.
        if hasattr(current_page, 'view_refresh_triggers') and selected_idx in current_page.view_refresh_triggers:
            refresh_func = current_page.view_refresh_triggers[selected_idx]
            if callable(refresh_func):
                print(f"DEBUG: Calling stored refresh/load function for view index {selected_idx}: {refresh_func}")
                refresh_func()
            else:
                print(f"DEBUG: Stored refresh_func for view index {selected_idx} is not callable.")
        # else: # Optional: if you want to log when no trigger is found for a view
            # print(f"DEBUG: No refresh trigger found on page for view index {selected_idx}.")

    page.switch_view_callback = lambda idx: switch_view(idx, page)
    page.global_switch_view_callback = switch_view
    nav_rail.on_change = lambda e: switch_view(e.control.selected_index, page)
    
    # Main layout structure: Row [NavRail, MainContent]
    # This will be wrapped in a Stack to position the chip
    main_layout_row = ft.Row(
        [
            nav_rail,
            ft.VerticalDivider(width=1),
            ft.Container( 
                content=main_content_column,
                expand=True,
                padding=ft.padding.all(15), 
            )
        ],
        expand=True,
        vertical_alignment=ft.CrossAxisAlignment.START 
    )
    
    page.add(
        ft.Stack(
            [
                main_layout_row, # Base layer
                ft.Container( # Container for the chip
                    content=db_display_chip,
                    right=10,
                    bottom=10,
                    padding=ft.padding.all(5), # Optional: for some spacing around the chip
                    # bgcolor=ft.colors.with_opacity(0.8, ft.colors.SURFACE_VARIANT), # Optional: slight background
                    # border_radius=15 # Optional: rounded corners for the container
                )
            ],
            expand=True
        )
    )

    switch_view(0, page) 
    update_active_db_display(None) # Initialize chip display for "no DB" state

if __name__ == "__main__":
    ft.app(target=main) 