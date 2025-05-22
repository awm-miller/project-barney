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
    from pipeline_scripts.playlist_processor import process_playlist
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
# Define SUBTITLES_BASE_DIR for playlist processor (can be within APP_DATA_DIR or DATABASES_DIR)
# Let's place it inside DATABASES_DIR, organized by DB name
SUBTITLES_BASE_DIR_PARENT = DATABASES_DIR # Or APP_DATA_DIR / "subtitle_cache"

KNOWN_DATABASES_FILE = APP_DATA_DIR / "known_databases.txt"
LAST_OPENED_DB_FILE = APP_DATA_DIR / "last_opened_db.txt" # For storing last opened DB

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

# --- Refs for View Database Sorting ---
sort_column_ref = ft.Ref[Optional[str]]() # Stores column name like 'title' or 'published_at'
sort_ascending_ref = ft.Ref[bool]()      # True for ASC, False for DESC

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
        
        if db_path: 
            db_display_name = db_path.stem 
            db_chip.leading = ft.Icon(name="storage", color="primary", size=18)
            # Ensure label is a Text control and set its value
            if not isinstance(db_chip.label, ft.Text):
                db_chip.label = ft.Text(db_display_name, size=12, overflow=ft.TextOverflow.ELLIPSIS, no_wrap=True)
            else:
                db_chip.label.value = db_display_name
                db_chip.label.size = 12
                db_chip.label.overflow = ft.TextOverflow.ELLIPSIS
                db_chip.label.no_wrap = True
            
            db_chip.tooltip = f"Active database: {db_display_name}"
        else: # No DB Path
            db_chip.leading = ft.Row(
                [
                    ft.Icon(name="storage_outlined", opacity=0.7, size=18), 
                    ft.Icon(name="cancel_outlined", color="error", size=18, tooltip="No database selected")
                ], 
                spacing=4, 
                vertical_alignment=ft.CrossAxisAlignment.CENTER
            )
            # Ensure label is an empty Text control when no DB is active
            if not isinstance(db_chip.label, ft.Text):
                db_chip.label = ft.Text("")
            else:
                db_chip.label.value = ""
            db_chip.tooltip = "No database active"
        
        db_chip.visible = True 
        db_chip.update()

def open_database(db_path_str: str, page: ft.Page):
    db_path = Path(db_path_str)
    if page_ref.current and hasattr(page_ref.current, 'active_db_path'):
        page_ref.current.active_db_path = db_path
        update_active_db_display(db_path)
        save_last_opened_db(db_path) # Save on successful open
        
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

# --- Function for fetching video data for View Database page ---
def fetch_videos_for_view(db_path_str: str, search_term: Optional[str] = None, 
                          page_number: int = 1, page_size: int = 10, 
                          sort_by: Optional[str] = 'last_updated_at', # Default sort column
                          sort_direction: str = 'DESC') -> Dict: # Default sort direction
    """ Fetches paginated and sorted video data from the database. """
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
            "ai_analysis_content", 
            "subtitle_file_path", # Corrected column name
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
        
        # Validate sort_by to prevent SQL injection if it were user-provided directly
        # Here, we control it internally, but good practice for future changes.
        allowed_sort_columns = ["title", "published_at", "last_updated_at", "video_id", "channel_id"]
        if sort_by not in allowed_sort_columns:
            sort_by = 'last_updated_at' # Default to a safe column
        if sort_direction.upper() not in ['ASC', 'DESC']:
            sort_direction = 'DESC' # Default to a safe direction

        paginated_query_str = f"SELECT {select_cols_str} {base_query} ORDER BY {sort_by} {sort_direction} LIMIT ? OFFSET ?"
        
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
    """ Shows video details, using ft.AlertDialog via page.overlay. """
    print(f"DEBUG: show_video_details_dialog (AlertDialog via overlay) for: {video_data.get('video_id', 'N/A')}")
    
    dialog_instance_ref = ft.Ref[ft.AlertDialog]()

    def close_the_alert_dialog(e):
        print(f"DEBUG: close_the_alert_dialog called. Dialog ref: {dialog_instance_ref.current}")
        if dialog_instance_ref.current:
            print(f"DEBUG: Dialog open state BEFORE close: {dialog_instance_ref.current.open}")
            dialog_instance_ref.current.open = False
            print(f"DEBUG: Dialog open state AFTER setting to False: {dialog_instance_ref.current.open}")
            
            # Ensure page updates to hide the dialog
            page.update()
            print(f"DEBUG: page.update() called in close_the_alert_dialog.")

            # Optional: Clean up from overlay after it's hidden
            # This might require another page.update() if done here.
            # For now, let's ensure it hides first.
            # if dialog_instance_ref.current in page.overlay:
            #     print(f"DEBUG: Removing dialog from overlay.")
            #     page.overlay.remove(dialog_instance_ref.current)
            #     page.update() # Update again after removal

    # Original dialog content restoration
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

    # Create ExpansionPanels for AI Analysis and Transcript
    ai_analysis_panel = ft.ExpansionPanel(
        header=ft.Container(ft.Text("AI Analysis", style=ft.TextThemeStyle.TITLE_MEDIUM), padding=ft.padding.only(left=10, top=5, bottom=5)),
        content=ft.Container(
            ft.Text(video_data.get('ai_analysis_content', 'No AI analysis available.'), selectable=True),
            padding=ft.padding.all(10)
        ),
        # expanded=True # Optionally start expanded
    )

    transcript_panel = ft.ExpansionPanel(
        header=ft.Container(ft.Text("Transcript", style=ft.TextThemeStyle.TITLE_MEDIUM), padding=ft.padding.only(left=10, top=5, bottom=5)),
        content=ft.Container(
            ft.Text(transcript_content_text, selectable=True),
            padding=ft.padding.all(10),
            height=150, # Keep scrollable height if content is long
        ),
        # expanded=True
    )

    dialog_content_column = ft.Column(
        [
            ft.ExpansionPanelList(
                controls=[
                    ai_analysis_panel,
                    transcript_panel
                ],
                elevation=1, # Optional: add a slight shadow
                divider_color="outlinevariant" # Corrected: string literal for Material 3 color role
            )
        ],
        tight=True, 
        width=600, 
        scroll=ft.ScrollMode.ADAPTIVE 
    )
    # End of original dialog content restoration

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
        actions=None, # Removed actions, close button is in the title now
        actions_alignment=ft.MainAxisAlignment.END,
        open=False 
    )
    
    # Add to overlay if not already there (e.g., if reusing instances was a concept)
    # For a new dialog each time, just append.
    if alert_dialog not in page.overlay:
        page.overlay.append(alert_dialog)
    # page.update() # Update to add to overlay, might not be strictly needed before open

    alert_dialog.open = True
    page.update()

    print(f"DEBUG: AlertDialog added to overlay and opened. Open state: {alert_dialog.open}")

# The close_dialog function is for AlertDialog, not strictly needed for BottomSheet if using page.close_bottom_sheet()
# but we can keep it if we plan to reuse/revert
def close_dialog(page, dialog_instance): # This is for AlertDialog
    if isinstance(dialog_instance, ft.AlertDialog):
        dialog_instance.open = False
        page.update()
    # For BottomSheet, page.close_bottom_sheet() is preferred.

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
    db_name_field = ft.TextField(
        label="New Database Name (e.g., 'tech_channels')", 
        width=350, # Increased width slightly
        border=ft.InputBorder.OUTLINE,
        border_radius=8,
        # hint_text="Enter a name for your new database collection."
    )
    create_db_view_content_ref = ft.Ref[ft.Column]()

    def create_db_action(e):
        print("DEBUG: create_db_action started.")
        db_name_raw = db_name_field.value
        if not db_name_raw:
            page.snack_bar = ft.SnackBar(ft.Text("Database name cannot be empty."), open=True, bgcolor="errorcontainer")
            page.update()
            print("DEBUG: create_db_action exited - empty name.")
            return

        db_name_sanitized = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in db_name_raw)
        if not db_name_sanitized:
             page.snack_bar = ft.SnackBar(ft.Text("Invalid characters in database name. Use alphanumeric, underscore, or hyphen."), open=True, bgcolor="errorcontainer")
             page.update()
             print("DEBUG: create_db_action exited - invalid sanitized name.")
             return

        if not db_name_sanitized.endswith(".db"):
            db_filename = db_name_sanitized + ".db"
        else:
            db_filename = db_name_sanitized
        
        new_db_path = DATABASES_DIR / db_filename

        if new_db_path.exists():
            page.snack_bar = ft.SnackBar(ft.Text(f"Database '{db_filename}' already exists."), open=True, bgcolor="warningcontainer")
            page.update()
            print(f"DEBUG: create_db_action exited - DB '{db_filename}' already exists.")
            return

        try:
            print(f"DEBUG: Attempting to create and initialize DB: {new_db_path}")
            conn = sqlite3.connect(new_db_path) 
            try:
                from pipeline_scripts.database_manager import initialize_database
                initialize_database(str(new_db_path))
                print(f"DEBUG: Database {new_db_path} initialized with tables via initialize_database.")
            except ImportError as init_ex:
                print(f"WARNING: Could not import initialize_database ({init_ex}). Manually creating 'videos' table for {new_db_path}")
                cursor = conn.cursor()
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, video_id TEXT UNIQUE NOT NULL, video_url TEXT, 
                    channel_id TEXT, title TEXT, published_at TIMESTAMP,
                    subtitle_status TEXT, subtitle_file_path TEXT, plain_text_subtitle_path TEXT,
                    subtitle_fetched_at TIMESTAMP, subtitle_to_text_status TEXT, subtitle_to_text_completed_at TIMESTAMP,
                    source_script TEXT, status TEXT, last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                ''')
                conn.commit()
                print(f"DEBUG: Manually created minimal 'videos' table for {new_db_path}.")
            conn.close()
            print(f"DEBUG: DB file {new_db_path} created and connection closed.")
            
            add_known_database(new_db_path, page)
            print(f"DEBUG: DB {new_db_path} added to known databases.")
            open_database(str(new_db_path), page) 
            print(f"DEBUG: DB {new_db_path} opened.")
            
            page.snack_bar = ft.SnackBar(ft.Text(f"Database '{db_filename}' created. Now choose how to populate it."), open=True)
            db_name_field.value = "" 
            # db_name_field.update() # No need to update field if the whole view is changing
            
            print("DEBUG: Attempting to transition to Choose Import Method view.")
            if main_content_area_ref.current:
                print("DEBUG: main_content_area_ref.current is available.")
                main_content_area_ref.current.controls.clear()
                main_content_area_ref.current.controls.append(build_choose_import_method_view(page, new_db_path))
                main_content_area_ref.current.update()
                page.update() # Ensure page updates after content area manipulation
                print("DEBUG: Switched to Choose Import Method view.")
            else:
                print("ERROR: main_content_area_ref.current is NOT available. Cannot switch view.")
                if hasattr(page, 'switch_view_callback'):
                    print("DEBUG: Fallback - switching to home view (index 0).")
                    page.switch_view_callback(0) 
                page.update() # Update page even in fallback

        except Exception as ex:
            page.snack_bar = ft.SnackBar(ft.Text(f"Error creating database: {ex}"), open=True, bgcolor="errorcontainer")
            page.update()
            print(f"ERROR: Exception in create_db_action: {ex}")

    create_db_view_column = ft.Column(
        ref=create_db_view_content_ref,
        controls=[
            ft.Text("Create New Database - Step 1: Name", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM),
            ft.Text(f"Databases will be saved in: {DATABASES_DIR.resolve()}"),
            db_name_field,
            ft.ElevatedButton("Next: Choose Import Method", icon="arrow_forward", on_click=create_db_action)
        ],
        spacing=20,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER
    )
    return create_db_view_column

def build_choose_import_method_view(page: ft.Page, db_path: Path):
    """Builds the view for choosing how to populate the new database."""
    
    def go_to_playlist_import(e):
        if main_content_area_ref.current:
            main_content_area_ref.current.controls.clear()
            main_content_area_ref.current.controls.append(build_add_by_playlist_view(page, db_path))
            main_content_area_ref.current.update()

    def go_to_date_import(e): # Placeholder
        page.snack_bar = ft.SnackBar(ft.Text("Import by date (random) is not yet implemented."), open=True)
        page.update()

    return ft.Column(
        [
            ft.Text(f"Populate '{db_path.name}' - Step 2: Choose Import Method", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM),
            ft.Text("How would you like to add videos to this new database?"),
            ft.ElevatedButton("Import from YouTube Playlist", icon="playlist_add", on_click=go_to_playlist_import, width=300),
            ft.ElevatedButton("Import by Date (Random - Placeholder)", icon="event", on_click=go_to_date_import, width=300, disabled=True), # Disabled for now
            ft.Divider(height=20),
            ft.TextButton("Or, skip and manage this database later", on_click=lambda e: page.switch_view_callback(0) if hasattr(page, 'switch_view_callback') else None)
        ],
        spacing=20,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        # alignment=ft.MainAxisAlignment.START, # Let content flow from top
        expand=True, # Allow this view to expand
        scroll=ft.ScrollMode.ADAPTIVE # Add scroll to the whole view if needed
    )

def build_add_by_playlist_view(page: ft.Page, db_path: Path):
    """Builds the view for adding videos by playlist URL."""
    playlist_url_field = ft.TextField(label="YouTube Playlist URL", width=500)
    progress_messages_column = ft.Column(scroll=ft.ScrollMode.ADAPTIVE, height=200, spacing=5) # Area for messages
    # Overall progress bar for the playlist
    playlist_progress_bar = ft.ProgressBar(value=0, visible=False, width=page.width*0.8 if page.width else 500)
    # Text for current video progress
    current_video_progress_text = ft.Text("", visible=False)
    start_import_button_ref = ft.Ref[ft.ElevatedButton]()


    def playlist_progress_callback(message_type: str, data: dict):
        # This function is called from the background thread via page.call_soon_threadsafe
        
        def _update_ui():
            nonlocal current_total_videos # Access the variable from the outer scope
            if message_type == "playlist_start":
                progress_messages_column.controls.append(ft.Text(f"Starting playlist processing for: {data.get('playlist_url')}"))
                playlist_progress_bar.visible = True
                current_video_progress_text.visible = True
            elif message_type == "error":
                progress_messages_column.controls.append(ft.Text(f"ERROR: {data.get('message')}", color="error"))
                if start_import_button_ref.current: start_import_button_ref.current.disabled = False # Re-enable button on error
            elif message_type == "playlist_fetch_items":
                progress_messages_column.controls.append(ft.Text(data.get("status", "Fetching playlist items...")))
            elif message_type == "playlist_fetch_items_success":
                current_total_videos = data.get("count", 0)
                progress_messages_column.controls.append(ft.Text(f"Found {current_total_videos} videos in playlist."))
                if current_total_videos == 0:
                     if start_import_button_ref.current: start_import_button_ref.current.disabled = False # Re-enable if no videos
                     playlist_progress_bar.visible = False
                     current_video_progress_text.visible = False


            elif message_type == "total_videos": # Handled by fetch_items_success for current_total_videos
                pass # current_total_videos is already set
            
            elif message_type == "video_processing_start":
                idx = data.get("index", 0)
                title = data.get("title", "Unknown video")
                # Update overall progress bar for playlist
                if current_total_videos > 0:
                    playlist_progress_bar.value = (idx + 1) / current_total_videos
                else:
                    playlist_progress_bar.value = 0
                
                current_video_progress_text.value = f"Processing video {idx+1}/{current_total_videos}: '{title}' (0%)"
            
            elif message_type == "video_progress":
                idx = data.get("index", 0)
                task = data.get("task", "working...")
                percentage = data.get("percentage", 0)
                title = data.get("title", progress_messages_column.controls[-1].value if progress_messages_column.controls else "current video") # try to get title
                # Try to get title from a previous message if possible, or use a generic term
                current_video_progress_text.value = f"Video {idx+1}/{current_total_videos}: {task} ({percentage}%)"

            elif message_type == "video_completed":
                idx = data.get("index", 0)
                video_id = data.get("video_id", "N/A")
                details = data.get("details", {})
                title = details.get("title", video_id)
                progress_messages_column.controls.append(ft.Text(f"  [OK] Video {idx+1}: '{title}' processed and added to DB.", color="green"))
                current_video_progress_text.value = f"Video {idx+1}/{current_total_videos}: Completed."
                if (idx + 1) == current_total_videos: # Last video
                    playlist_progress_bar.value = 1

            elif message_type == "video_error":
                idx = data.get("index", 0)
                video_id = data.get("video_id", "N/A")
                error_msg = data.get("error", "Unknown error")
                progress_messages_column.controls.append(ft.Text(f"  [FAIL] Video {idx+1} ({video_id}): {error_msg}", color="error"))
                current_video_progress_text.value = f"Video {idx+1}/{current_total_videos}: Error."
                # Overall progress bar still advances as we tried to process it.
                if current_total_videos > 0:
                     playlist_progress_bar.value = (idx + 1) / current_total_videos

            elif message_type == "all_completed":
                total_processed = data.get("total_processed", "N/A")
                progress_messages_column.controls.append(ft.Text(f"Playlist processing finished. Processed {total_processed} videos.", weight=ft.FontWeight.BOLD))
                playlist_progress_bar.value = 1
                current_video_progress_text.value = "All videos processed."
                if start_import_button_ref.current: start_import_button_ref.current.disabled = False # Re-enable button
            
            # Ensure controls are updated on the page
            progress_messages_column.update()
            playlist_progress_bar.update()
            current_video_progress_text.update()
            if start_import_button_ref.current: start_import_button_ref.current.update()
            page.update() # General page update to reflect changes

        # Schedule the UI update to run on Flet's main event loop
        page.call_soon_threadsafe(_update_ui)

    current_total_videos = 0 # Variable to store total videos for progress calculation

    def start_playlist_import_action(e):
        nonlocal current_total_videos # Allow modification
        current_total_videos = 0 # Reset for new import

        url = playlist_url_field.value
        if not url:
            page.snack_bar = ft.SnackBar(ft.Text("Playlist URL cannot be empty."), open=True, bgcolor="errorcontainer")
            page.update()
            return

        # Disable button, clear previous messages
        if start_import_button_ref.current: start_import_button_ref.current.disabled = True
        progress_messages_column.controls.clear()
        progress_messages_column.controls.append(ft.Text(f"Starting import for playlist: {url}"))
        progress_messages_column.update()
        playlist_progress_bar.value = 0
        playlist_progress_bar.visible = True
        playlist_progress_bar.update()
        current_video_progress_text.value = "Initializing..."
        current_video_progress_text.visible = True
        current_video_progress_text.update()
        page.update()

        # Define subtitle storage directory based on the DB name
        # e.g., databases/my_playlist_db_subtitles/
        db_name_stem = db_path.stem 
        subtitles_for_this_db_dir = SUBTITLES_BASE_DIR_PARENT / f"{db_name_stem}_subtitles"
        
        try:
            subtitles_for_this_db_dir.mkdir(parents=True, exist_ok=True)
        except Exception as ex_mkdir:
            page.snack_bar = ft.SnackBar(ft.Text(f"Error creating subtitle directory: {ex_mkdir}"), open=True, bgcolor="errorcontainer")
            if start_import_button_ref.current: start_import_button_ref.current.disabled = False
            page.update()
            return

        page.run_thread_async(
            process_playlist,
            playlist_url=url,
            db_path=str(db_path),
            subtitle_base_dir=str(subtitles_for_this_db_dir),
            progress_callback=playlist_progress_callback
        )

    return ft.Column(
        [
            ft.Text(f"Populate '{db_path.name}' - Step 3: Add by Playlist", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM),
            playlist_url_field,
            ft.ElevatedButton("Start Import", ref=start_import_button_ref, icon="cloud_download", on_click=start_playlist_import_action),
            ft.Divider(height=15),
            ft.Text("Overall Playlist Progress:", weight=ft.FontWeight.BOLD),
            playlist_progress_bar,
            ft.Text("Current Video Progress:", weight=ft.FontWeight.BOLD, visible=False), # Initially hidden
            current_video_progress_text, # Displays detailed progress of current video
            ft.Text("Import Log:", weight=ft.FontWeight.BOLD),
            ft.Container(
                content=progress_messages_column,
                border=ft.border.all(1, "grey"), # Corrected color to string literal
                border_radius=5,
                padding=10,
                expand=True, # Allow it to take available vertical space if Column is expanded
                height=300 # Fixed height for scrollable area
            ),
            ft.TextButton("Back to Import Methods", on_click=lambda e: (
                main_content_area_ref.current.controls.clear(),
                main_content_area_ref.current.controls.append(build_choose_import_method_view(page, db_path)),
                main_content_area_ref.current.update()
            ) if main_content_area_ref.current else None),
            ft.TextButton("Finish and go to Home", on_click=lambda e: page.switch_view_callback(0) if hasattr(page, 'switch_view_callback') else None)

        ],
        spacing=15,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        # alignment=ft.MainAxisAlignment.START, # Let content flow from top
        expand=True, # Allow this view to expand
        scroll=ft.ScrollMode.ADAPTIVE # Add scroll to the whole view if needed
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
                ft.Text("Manage Databases", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM, expand=True), # Title takes available space
                ft.Row([ # Group for buttons on the right
                    # ft.IconButton("refresh", on_click=lambda e_click: local_trigger_db_list_refresh(), tooltip="Refresh List"), # Removed refresh button
                    ft.ElevatedButton("Import", icon="add_circle_outline", on_click=add_existing_db_dialog)
                ], spacing=5)
            ], alignment=ft.MainAxisAlignment.START, vertical_alignment=ft.CrossAxisAlignment.CENTER), # Main row alignment
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
        ft.DataColumn(ft.Text("Link")), 
        ft.DataColumn(ft.Text("Title")),
        ft.DataColumn(ft.Text("Published")), 
        ft.DataColumn(ft.Text("Actions"), numeric=True), # Actions often better numeric for right-align if desired
    ]

    data_table = ft.DataTable(
        columns=datatable_columns,
        rows=[],
        column_spacing=20, # Increased spacing a bit
        divider_thickness=0.5,
        # expand=True, # Removed expand
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
                    
                    youtube_icon = ft.Icon(name="smart_display", color="red", size=20, tooltip="Open YouTube link") # Increased size slightly

                    # Wrap the Icon in a Container to make it clickable
                    clickable_youtube_icon_container = ft.Container(
                        content=youtube_icon,
                        on_click=lambda e, url=video_url: page.launch_url(url),
                        ink=True, 
                        border_radius=4,
                        padding=ft.padding.all(2) # Small padding so click area is decent around icon
                    )

                    # Format published date
                    published_at_str = video_data.get('published_at')
                    formatted_date = "N/A"
                    if published_at_str:
                        try:
                            # Replace Z with +00:00 for fromisoformat if Z is used for UTC
                            if published_at_str.endswith('Z'):
                                published_at_str = published_at_str[:-1] + '+00:00'
                            dt_obj = datetime.fromisoformat(published_at_str)
                            formatted_date = dt_obj.strftime("%d/%m/%Y")
                        except ValueError:
                            # Fallback for simpler "YYYY-MM-DD" or if fromisoformat fails
                            try:
                                date_part = published_at_str.split('T')[0].split(' ')[0] 
                                dt_obj = datetime.strptime(date_part, "%Y-%m-%d")
                                formatted_date = dt_obj.strftime("%d/%m/%Y")
                            except ValueError:
                                formatted_date = published_at_str 
                                print(f"Could not parse date: {published_at_str}") 

                    cells = [
                        ft.DataCell(clickable_youtube_icon_container),
                        ft.DataCell(ft.Text(video_data.get('title', 'N/A'), overflow=ft.TextOverflow.ELLIPSIS)),
                        ft.DataCell(ft.Text(formatted_date)),
                        ft.DataCell(ft.IconButton(icon="visibility", tooltip="View Details", 
                                                  on_click=lambda e, vd=video_data: (
                                                      print(f"DEBUG: Eye icon clicked for video ID: {vd.get('video_id')}"), 
                                                      show_video_details_dialog(page, vd)
                                                  )
                                     ))
                    ]
                    data_table_ref.current.rows.append(ft.DataRow(cells=cells))
            data_table_ref.current.update()
        
        update_pagination_controls()

    def _fetch_and_update_page_data(page_num_to_load: int, search_term_val: Optional[str]):
        if page.active_db_path and current_db_page_ref.current is not None and db_page_size_ref.current is not None and sort_column_ref.current is not None and sort_ascending_ref.current is not None:
            current_db_page_ref.current = page_num_to_load 
            
            sort_dir_str = 'ASC' if sort_ascending_ref.current else 'DESC'

            fetched_data = fetch_videos_for_view(
                str(page.active_db_path), 
                search_term_val,
                page_number=current_db_page_ref.current,
                page_size=db_page_size_ref.current,
                sort_by=sort_column_ref.current,
                sort_direction=sort_dir_str
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
        # Initialize sort state if not already set by a click or previous load
        if sort_column_ref.current is None: sort_column_ref.current = 'published_at'
        if sort_ascending_ref.current is None: sort_ascending_ref.current = False
        
        search_term = search_field.value 
        _fetch_and_update_page_data(1, search_term) 
        update_column_header_visuals() # Set initial sort indicators

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
    if db_page_size_ref.current is None: db_page_size_ref.current = 10
    if sort_column_ref.current is None: sort_column_ref.current = 'published_at' # Default sort
    if sort_ascending_ref.current is None: sort_ascending_ref.current = False # Default DESC for dates

    # Refs for column header elements to update sort indicators
    title_header_text_ref = ft.Ref[ft.Text]()
    title_header_icon_ref = ft.Ref[ft.Icon]()
    published_header_text_ref = ft.Ref[ft.Text]()
    published_header_icon_ref = ft.Ref[ft.Icon]()

    def update_column_header_visuals():
        # Reset both icons first
        if title_header_icon_ref.current: 
            title_header_icon_ref.current.name = "unfold_more" # Default icon (or None/empty string for no icon)
            title_header_icon_ref.current.visible = False # Or True if using unfold_more
        if published_header_icon_ref.current:
            published_header_icon_ref.current.name = "unfold_more"
            published_header_icon_ref.current.visible = False

        target_icon_ref = None
        if sort_column_ref.current == 'title':
            target_icon_ref = title_header_icon_ref
        elif sort_column_ref.current == 'published_at':
            target_icon_ref = published_header_icon_ref

        if target_icon_ref and target_icon_ref.current:
            target_icon_ref.current.name = "arrow_upward" if sort_ascending_ref.current else "arrow_downward"
            target_icon_ref.current.visible = True
        
        if title_header_icon_ref.current: title_header_icon_ref.current.update()
        if published_header_icon_ref.current: published_header_icon_ref.current.update()

    def handle_sort_click(column_name: str):
        if sort_column_ref.current == column_name:
            sort_ascending_ref.current = not sort_ascending_ref.current
        else:
            sort_column_ref.current = column_name
            sort_ascending_ref.current = True if column_name == 'title' else False 
        
        current_db_page_ref.current = 1 
        _fetch_and_update_page_data(current_db_page_ref.current, search_field.value)
        update_column_header_visuals()

    # Define DataTable columns with interactive labels for sortable columns
    datatable_columns = [
        ft.DataColumn(ft.Text("Link")), 
        ft.DataColumn(
            ft.Container(
                content=ft.Row([ 
                    ft.Text("Title", ref=title_header_text_ref), 
                    ft.Icon(ref=title_header_icon_ref, size=16, visible=False)
                ], alignment=ft.MainAxisAlignment.START, spacing=4),
                on_click=lambda e: handle_sort_click('title'),
                ink=True, border_radius=4, padding=ft.padding.symmetric(horizontal=4) # Make area clickable
            )
        ),
        ft.DataColumn(
            ft.Container(
                content=ft.Row([
                    ft.Text("Published", ref=published_header_text_ref), 
                    ft.Icon(ref=published_header_icon_ref, size=16, visible=False)
                ], alignment=ft.MainAxisAlignment.START, spacing=4),
                on_click=lambda e: handle_sort_click('published_at'),
                ink=True, border_radius=4, padding=ft.padding.symmetric(horizontal=4)
            )
        ),
        ft.DataColumn(ft.Text("Actions"), numeric=True),
    ]

    data_table = ft.DataTable(
        columns=datatable_columns,
        rows=[],
        column_spacing=20, # Increased spacing a bit
        divider_thickness=0.5,
        # expand=True, # Removed expand
    )
    
    # Using a Ref for data_table to update its rows
    data_table_ref = ft.Ref[ft.DataTable]()
    data_table_ref.current = data_table

    # Container for the DataTable to allow horizontal scrolling if needed
    table_container = ft.Row(
        [data_table],
        scroll=ft.ScrollMode.ADAPTIVE, 
        # expand=True # Removed expand
    )

    return ft.Column(
        [
            ft.Column( # Inner column for content, this will be centered by the outer column
                [
                    ft.Text("View Database Content", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM),
                    ft.Row([search_field, ft.ElevatedButton("Search", icon="search", on_click=lambda e: perform_search(e, is_new_search=True))], alignment=ft.MainAxisAlignment.CENTER), # Center search bar too
                    ft.Divider(),
                    table_container, 
                    pagination_row 
                ],
                spacing=10, 
                horizontal_alignment=ft.CrossAxisAlignment.CENTER # Changed from STRETCH to CENTER
            )
        ],
        expand=True, 
        horizontal_alignment=ft.CrossAxisAlignment.CENTER, 
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

# --- Save and Load Last Opened Database ---
def save_last_opened_db(db_path: Path):
    if APP_DATA_DIR.exists(): # Should always exist due to earlier mkdir
        with open(LAST_OPENED_DB_FILE, "w") as f:
            f.write(str(db_path.resolve()))

def load_last_opened_db() -> Optional[Path]:
    if LAST_OPENED_DB_FILE.exists():
        try:
            with open(LAST_OPENED_DB_FILE, "r") as f:
                path_str = f.read().strip()
            
            if path_str:
                db_path = Path(path_str)
                if db_path.exists() and db_path.is_file(): # Verify it still exists
                    return db_path
                else:
                    # If path is invalid or file doesn't exist, try to clear it
                    print(f"INFO: Last opened DB path '{path_str}' is invalid or file missing. Attempting to remove {LAST_OPENED_DB_FILE}")
                    try:
                        LAST_OPENED_DB_FILE.unlink(missing_ok=True)
                        print(f"INFO: Successfully removed {LAST_OPENED_DB_FILE}.")
                    except PermissionError as e:
                        print(f"WARNING: Could not remove {LAST_OPENED_DB_FILE} due to PermissionError: {e}. The app will continue.")
                    except Exception as e_unlink: # Catch other potential errors during unlink
                        print(f"WARNING: Error removing {LAST_OPENED_DB_FILE}: {e_unlink}. The app will continue.")
        except Exception as e_read: # Catch errors during reading the file itself
            print(f"WARNING: Error reading {LAST_OPENED_DB_FILE}: {e_read}. Attempting to remove the problematic file.")
            try:
                LAST_OPENED_DB_FILE.unlink(missing_ok=True)
            except Exception as e_unlink_on_read_error:
                print(f"WARNING: Could not remove problematic {LAST_OPENED_DB_FILE} after read error: {e_unlink_on_read_error}")
    return None

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

    # --- Scrollbar Theming (Attempt) ---
    # Removing scrollbar theming as ScrollbarThemeData is not available in older Flet versions
    # page.theme = ft.Theme(
    #     scrollbar_theme=ft.ScrollbarThemeData(
    #         thumb_visibility=False, 
    #         thickness=5,          
    #         radius=5,
    #         thumb_color={ft.MaterialState.HOVERED: ft.colors.with_opacity(0.5, ft.colors.WHITE70), 
    #                      ft.MaterialState.DEFAULT: ft.colors.with_opacity(0.2, ft.colors.WHITE30)},
    #         interactive=True 
    #     )
    # )
    # page.dark_theme = ft.Theme(
    #     scrollbar_theme=ft.ScrollbarThemeData(
    #         thumb_visibility=False, 
    #         thickness=5,          
    #         radius=5,
    #         thumb_color={ft.MaterialState.HOVERED: ft.colors.with_opacity(0.5, ft.colors.WHITE70), 
    #                      ft.MaterialState.DEFAULT: ft.colors.with_opacity(0.2, ft.colors.WHITE30)},
    #         interactive=True 
    #     )
    # ) 

    # Database Display Chip - will be positioned in a Stack later
    db_display_chip = ft.Chip(
        ref=active_db_chip_ref,
        label=ft.Text(""), # Use an empty Text control instead of None
        visible=True, 
        on_click=lambda e: page.switch_view_callback(2) if hasattr(page, 'switch_view_callback') else None # Index 2 is Change DB view builder
    )

    # Navigation Rail (Sidebar)
    nav_rail = ft.NavigationRail(
        selected_index=0,
        label_type=ft.NavigationRailLabelType.ALL,
        min_width=100, 
        min_extended_width=250, 
        group_alignment=-0.9, 
        destinations=[
            ft.NavigationRailDestination(icon="home_outlined", selected_icon="home", label="Home"),
            ft.NavigationRailDestination(icon="create_new_folder_outlined", selected_icon="create_new_folder", label="New DB"),
            # Change DB (index 2 in view_builders) is now accessed via chip
            ft.NavigationRailDestination(icon="table_chart_outlined", selected_icon="table_chart", label="View DB"),     # NavRail idx 2
            ft.NavigationRailDestination(icon="model_training_outlined", selected_icon="model_training", label="AI Analysis"), # NavRail idx 3
            ft.NavigationRailDestination(icon="settings_outlined", selected_icon="settings", label="Settings"),               # NavRail idx 4
        ],
        trailing=None 
    )

    # Main content area (center part of the page)
    main_content_column = ft.Column(ref=main_content_area_ref, expand=True, spacing=20, scroll=ft.ScrollMode.ADAPTIVE)

    # Mapping from NavRail index to view_builders key
    nav_to_builder_map = {
        0: 0, # Home
        1: 1, # New DB
        2: 3, # View DB (was 3, now NavRail index 2)
        3: 4, # AI Analysis (was 4, now NavRail index 3)
        4: 5, # Settings (was 5, now NavRail index 4)
    }
    
    # View builders dictionary (index 2 is Change DB, accessed by chip)
    view_builders = {
        0: build_home_view,
        1: build_create_new_db_view,
        2: build_change_database_view, 
        3: build_view_database_view,
        4: build_run_ai_analysis_view,
        5: build_settings_view,
    }

    def switch_view(selected_builder_idx: int, current_page: ft.Page):
        main_content_column.controls.clear()
        builder = view_builders.get(selected_builder_idx)
        if builder:
            view_content = builder(current_page) 
            main_content_column.controls.append(view_content)
        else:
            main_content_column.controls.append(ft.Text(f"View builder for index {selected_builder_idx} not found."))
        main_content_column.update() 

        # Refresh trigger logic (based on builder_idx, which is what refresh_triggers are keyed by)
        if hasattr(current_page, 'view_refresh_triggers') and selected_builder_idx in current_page.view_refresh_triggers:
            refresh_func = current_page.view_refresh_triggers[selected_builder_idx]
            if callable(refresh_func):
                print(f"DEBUG: Calling stored refresh/load function for view builder index {selected_builder_idx}: {refresh_func}")
                refresh_func()
            else:
                print(f"DEBUG: Stored refresh_func for view builder index {selected_builder_idx} is not callable.")

    # Callback for chip (calls switch_view with builder index 2 for Change DB)
    page.switch_view_callback = lambda builder_idx: switch_view(builder_idx, page)
    
    # NavRail on_change now uses the mapping
    nav_rail.on_change = lambda e: switch_view(nav_to_builder_map.get(e.control.selected_index, 0), page) # Default to Home (builder idx 0)
    
    # Define main_layout_row before using it in ft.Stack
    main_layout_row = ft.Row(
        [
            nav_rail,
            ft.VerticalDivider(width=1),
            ft.Container( 
                content=main_content_column,
                expand=True,
                padding=ft.padding.only(left=15, right=15, bottom=15, top=55), # Added top padding to avoid chip
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
                    right=10, # Position top-right
                    top=10,
                    # bottom=10, # Remove bottom positioning
                    padding=ft.padding.all(5), # Optional: for some spacing around the chip
                    # bgcolor=ft.colors.with_opacity(0.8, ft.colors.SURFACE_VARIANT), # Optional: slight background
                    # border_radius=15 # Optional: rounded corners for the container
                )
            ],
            expand=True
        )
    )

    # Attempt to load and open the last used database
    last_db = load_last_opened_db()
    if last_db:
        open_database(str(last_db), page) 
        # After opening, directly go to home view, as the wizard is for new DBs
        if hasattr(page, 'switch_view_callback'):
             page.switch_view_callback(0) # Go to home
        else: # Fallback if callback not set yet
            switch_view(0, page)
    else:
        update_active_db_display(None) 
        switch_view(0, page) # Start at home page if no last DB

    # Initial view should be home, not the result of last_db logic if it changes view.
    # The switch_view(0, page) at the end of main or after last_db handling ensures this.
    # If last_db opens a DB and we want to stay on home, ensure switch_view(0, page) is called last.
    # The current logic seems okay: open_database updates display but doesn't switch view.
    # switch_view(0, page) is called if no last_db, or explicitly by last_db logic.

if __name__ == "__main__":
    ft.app(target=main) 