import flet as ft
import subprocess
import sys
import re
from pathlib import Path
from datetime import datetime
from src.config import PROJECT_ROOT
from src.utils.db_utils import add_known_database

PIPELINE_SCRIPTS_DIR = PROJECT_ROOT / "src" / "pipeline_scripts"
CREATE_DB_SCRIPT_PATH = PIPELINE_SCRIPTS_DIR / "create_custom_db.py"
FETCH_SUBTITLES_SCRIPT_PATH = PIPELINE_SCRIPTS_DIR / "fetch_subtitles.py"

def build_pipeline_progress_view(page: ft.Page):
    print(f"DEBUG: Building Pipeline Progress View. DB: {getattr(page, 'pipeline_db_name', 'NS')}, URL: {getattr(page, 'pipeline_playlist_url', 'NS')}, API: {bool(getattr(page, 'pipeline_api_key', None))}")
    log_output_ref = ft.Ref[ft.TextField]()
    progress_bar_ref = ft.Ref[ft.ProgressBar]()
    current_task_text_ref = ft.Ref[ft.Text]()

    def update_log_ui(message: str):
        if log_output_ref.current:
            current_log = log_output_ref.current.value or ""
            log_lines = current_log.splitlines()
            max_lines = 150 # Allow more lines for combined logs
            if len(log_lines) > max_lines:
                log_lines = log_lines[-max_lines:]
            new_log = "\n".join(log_lines) + "\n" + message if current_log else message
            log_output_ref.current.value = new_log.strip()
            try:
                log_output_ref.current.update()
            except Exception as e_log_update:
                print(f"Error updating log UI: {e_log_update}")
        else:
            print(message)

    def run_pipeline_scripts():
        db_name = getattr(page, 'pipeline_db_name', None)
        playlist_url = getattr(page, 'pipeline_playlist_url', None)
        api_key = getattr(page, 'pipeline_api_key', None)
        print(f"DEBUG: Progress View - run_pipeline_scripts started. DB: {db_name}, URL: {playlist_url}, API: {bool(api_key)}")

        if not db_name or not playlist_url:
            update_log_ui("Error: Missing database name or playlist URL. Cannot start pipeline.")
            if current_task_text_ref.current:
                current_task_text_ref.current.value = "Error: Missing required info!"
                current_task_text_ref.current.update()
            return

        page.pipeline_final_db_path = None # Initialize
        print("DEBUG: Progress View - Initialized page.pipeline_final_db_path = None")

        # --- Step 1: Create Database ---
        if current_task_text_ref.current:
            current_task_text_ref.current.value = "Step 1: Creating Database..."
            current_task_text_ref.current.update()
        if progress_bar_ref.current:
            progress_bar_ref.current.value = None # Indeterminate
            progress_bar_ref.current.update()
        
        update_log_ui(f"--- Starting Database Creation for '{db_name}' ---")
        create_db_command = [
            sys.executable,
            str(CREATE_DB_SCRIPT_PATH),
            "--db-name", db_name,
            "--playlist-url", playlist_url
        ]
        if api_key:
            create_db_command.extend(["--api-key", api_key])

        update_log_ui(f"Executing: {' '.join(create_db_command)}")
        print(f"DEBUG: Progress View - Executing DB creation: {' '.join(create_db_command)}")
        
        # Use errors='replace' for both stdout and stderr to handle encoding issues
        db_process = subprocess.Popen(
            create_db_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',  # Added errors='replace' to handle encoding issues
            cwd=PROJECT_ROOT
        )
        
        db_stdout_full = ""
        if db_process.stdout:
            for line in iter(db_process.stdout.readline, ''):
                if line:
                    db_stdout_full += line
                    update_log_ui(line.strip())
            db_process.stdout.close()
        
        db_stderr_full = ""
        if db_process.stderr:
            for line in iter(db_process.stderr.readline, ''):
                if line:
                    db_stderr_full += line
                    update_log_ui(f"DB ERROR: {line.strip()}")
            db_process.stderr.close()

        db_process.wait()
        update_log_ui(f"Database creation script finished with exit code: {db_process.returncode}.")
        print(f"DEBUG: Progress View - DB creation script exit code: {db_process.returncode}")

        if db_process.returncode == 0:
            final_db_path_match = re.search(r"FINAL_DB_PATH:(.+)", db_stdout_full)
            if final_db_path_match:
                actual_db_path = final_db_path_match.group(1).strip()
                update_log_ui(f"Successfully created database: {actual_db_path}")
                page.pipeline_final_db_path = actual_db_path # Store for completion page
                print(f"DEBUG: Progress View - DB created. page.pipeline_final_db_path: {page.pipeline_final_db_path}")
                
                try:
                    add_known_database(Path(actual_db_path).name, {
                        "path": actual_db_path,
                        "name": Path(actual_db_path).name,
                        "added_timestamp": datetime.now().isoformat()  # Changed from ft.datetime to datetime
                    })
                    update_log_ui(f"Added '{Path(actual_db_path).name}' to known databases.")
                except Exception as e_add_known:
                    update_log_ui(f"Warning: Could not add database to known list: {e_add_known}")

                # --- Step 2: Fetch Subtitles ---
                if current_task_text_ref.current:
                    current_task_text_ref.current.value = "Step 2: Fetching Subtitles..."
                    current_task_text_ref.current.update()
                if progress_bar_ref.current: # Reset progress for next step
                    progress_bar_ref.current.value = None 
                    progress_bar_ref.current.update()

                update_log_ui(f"\n--- Starting Subtitle Fetching for '{db_name}' ---")
                fetch_subs_command = [
                    sys.executable,
                    str(FETCH_SUBTITLES_SCRIPT_PATH),
                    "--db-name", db_name, # Use the original db_name argument
                    "--workers", "4"
                ]
                update_log_ui(f"Executing: {' '.join(fetch_subs_command)}")
                print(f"DEBUG: Progress View - Executing subtitle fetching: {' '.join(fetch_subs_command)}")
                
                # Use errors='replace' for subtitle process too
                subs_process = subprocess.Popen(
                    fetch_subs_command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding='utf-8',
                    errors='replace',  # Added errors='replace' to handle encoding issues
                    cwd=PROJECT_ROOT
                )

                if subs_process.stdout:
                    for line in iter(subs_process.stdout.readline, ''):
                        if line:
                            update_log_ui(line.strip())
                    subs_process.stdout.close()
                
                if subs_process.stderr:
                    for line in iter(subs_process.stderr.readline, ''):
                        if line:
                            update_log_ui(f"SUBTITLE ERROR: {line.strip()}")
                    subs_process.stderr.close()
                
                subs_process.wait()
                update_log_ui(f"Subtitle fetching script finished with exit code: {subs_process.returncode}.")
                print(f"DEBUG: Progress View - Subtitle script exit code: {subs_process.returncode}")

                if subs_process.returncode == 0:
                    update_log_ui("Subtitle fetching completed successfully!")
                else:
                    update_log_ui("Warning: Subtitle fetching script finished with errors.")
                
                page.go("/pipeline_complete") # Go to complete regardless of subtitle outcome if DB created
                print("DEBUG: Progress View - Navigating to /pipeline_complete")

            else:
                update_log_ui("Error: FINAL_DB_PATH not found in database creation script output.")
                print("DEBUG: Progress View - Error: FINAL_DB_PATH not found.")
                if current_task_text_ref.current: current_task_text_ref.current.value = "Error: DB Path Missing!"; current_task_text_ref.current.update()
                # Consider a specific error page or message here
        else:
            update_log_ui(f"Database creation script failed. Full Stderr:\n{db_stderr_full}")
            print(f"DEBUG: Progress View - Error: DB creation script failed. Exit code: {db_process.returncode}")
            if current_task_text_ref.current: current_task_text_ref.current.value = "Error: DB Creation Failed!"; current_task_text_ref.current.update()
            # Consider a specific error page or button to go back

    # Create the view's UI
    view = ft.Column(
        [
            ft.Text("Pipeline Processing", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM, weight=ft.FontWeight.BOLD),
            ft.Text(ref=current_task_text_ref, value="Initializing pipeline...", text_align=ft.TextAlign.CENTER, size=16),
            ft.Container(height=10),
            ft.ProgressBar(ref=progress_bar_ref, width=600, color="amber", bgcolor="#eeeeee", value=None),
            ft.Container(height=10),
            ft.TextField(
                ref=log_output_ref,
                label="Process Log",
                multiline=True,
                read_only=True,
                min_lines=15,
                max_lines=25, # Increased max lines
                width=750,
                text_size=12,
                border_color="primary",
            ),
            ft.Container(height=20),
            # No navigation buttons here, transition is automatic or on error
        ],
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        alignment=ft.MainAxisAlignment.CENTER,
        spacing=15,
        expand=True
    )

    # Start processing immediately after the view is built
    page.run_thread(run_pipeline_scripts)
    print("DEBUG: Progress View - Started pipeline processing thread")

    return view 