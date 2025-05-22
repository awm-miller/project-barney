import flet as ft
import threading
import os
import sys # Keep for sys.path manipulation if needed by other parts
from pathlib import Path # Keep for Path usage if needed

from src.config import PROMPTS
from src.utils.ui_utils import check_active_db_and_show_snackbar

# Attempt to import db_info_utils, batch_ai_analyzer remains placeholder
try:
    from pipeline_scripts.db_info_utils import get_database_summary
except ImportError:
    try:
        # Fallback for direct execution or different structure
        sys.path.append(str(Path(__file__).parent.parent.resolve())) # src directory
        from pipeline_scripts.db_info_utils import get_database_summary
    except ImportError as e_fallback_db_info:
        print(f"Fallback ImportError for db_info_utils: {e_fallback_db_info}")
        def get_database_summary(db_path):
            print("Warning: get_database_summary from db_info_utils is not available (import failed).")
            return {"video_count": "N/A (Import Failed)", "status": "Error loading info (Import Failed)"}

# Placeholder for batch_ai_analyzer
def run_batch_analysis(db_path, api_key, prompt_template_str, prompt_key_for_logging, max_workers, max_videos, progress_callback):
    print("Warning: run_batch_analysis from batch_ai_analyzer is not available (import removed).")
    if progress_callback:
        progress_callback(0, 0, 0, 0, "Error: batch_ai_analyzer.py not found (Import Removed).")
    return {"error": "batch_ai_analyzer.py not found (Import Removed)."}


# Retrieve GEMINI_API_KEY from environment
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

def build_run_ai_analysis_view(page: ft.Page):
    """Builds the view for running AI analysis on videos."""
    if not check_active_db_and_show_snackbar(page):
        return ft.Column(
            [ft.Text("Please select a database first to run AI analysis.", theme_style=ft.TextThemeStyle.TITLE_LARGE, text_align=ft.TextAlign.CENTER)],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            alignment=ft.MainAxisAlignment.CENTER,
            expand=True
        )
        
    db_summary = {}
    if hasattr(page, 'active_db_path') and page.active_db_path:
        # This will now attempt to use the imported get_database_summary or its fallback
        db_summary = get_database_summary(str(page.active_db_path)) 
    
    progress_bar_ref = ft.Ref[ft.ProgressBar]()
    status_text_ref = ft.Ref[ft.Text]()
    
    def update_progress_on_ui_thread(current, total, success, failed, message):
        """ Safely updates progress UI elements from a thread. """
        if progress_bar_ref.current and status_text_ref.current:
            progress = current / total if total > 0 else 0
            progress_bar_ref.current.value = progress
            status_text_ref.current.value = f"{message}\nProcessed: {current}/{total} (Success: {success}, Failed: {failed})"
            page.update() 

    def run_analysis_click(e):
        """Starts the analysis process."""
        if not GEMINI_API_KEY:
            page.snack_bar = ft.SnackBar(
                content=ft.Text("Gemini API key not found. Please set the GEMINI_API_KEY environment variable or add it in Settings."),
                bgcolor="errorcontainer"
            )
            page.snack_bar.open = True
            page.update()
            return

        if not hasattr(page, 'active_db_path') or not page.active_db_path:
            check_active_db_and_show_snackbar(page)
            return

        def analysis_thread():
            # Use imported get_database_summary if available, otherwise its fallback
            current_db_summary = get_database_summary(str(page.active_db_path))
            initial_status = "Starting analysis..."
            if "Import Failed" in current_db_summary.get("status", ""):
                 initial_status = "Starting analysis (DB summary import failed - using placeholder)..."
            elif "Import Removed" in current_db_summary.get("status", "") : # This case should not happen if we re-added the import
                 initial_status = "Starting analysis (DB summary import removed - using placeholder)..."

            if status_text_ref.current:
                 page.run_thread_safe(lambda: setattr(status_text_ref.current, 'value', initial_status))
                 page.run_thread_safe(lambda: status_text_ref.current.update())

            # run_batch_analysis still uses its placeholder
            result = run_batch_analysis(
                db_path=str(page.active_db_path),
                api_key=GEMINI_API_KEY,
                prompt_template_str=PROMPTS["themes"],
                prompt_key_for_logging="themes",
                max_workers=4,
                max_videos=None,
                progress_callback=lambda cur, tot, suc, fail, msg: page.run_thread_safe(
                    lambda: update_progress_on_ui_thread(cur, tot, suc, fail, msg)
                )
            )
            
            final_message = f"Analysis process finished (run_batch_analysis is a placeholder). Result: {result}"
            if "error" in result:
                 final_message = f"Analysis failed (Placeholder for run_batch_analysis): {result['error']}"
            
            def final_update():
                if status_text_ref.current:
                    status_text_ref.current.value = final_message
                if progress_bar_ref.current:
                    progress_bar_ref.current.value = 1
                    if "error" in result:
                         progress_bar_ref.current.color = "error"
                    else:
                         progress_bar_ref.current.color = "success"
                page.snack_bar = ft.SnackBar(ft.Text(final_message), open=True)
                page.update()
            
            page.run_thread_safe(final_update)

        thread = threading.Thread(target=analysis_thread)
        thread.daemon = True
        thread.start()

    # UI text should reflect the state of imports
    db_summary_text = f"Videos in database: {db_summary.get('video_count', 'N/A')}"
    if "Import Failed" in db_summary.get("status", ""):
        db_summary_text += " (DB info import failed)"
    
    tooltip_text = "Uses the 'themes' prompt for analysis."
    status_idle_text = "Status: Idle"
    # Check if run_batch_analysis is the placeholder by inspecting its __doc__ or a unique string in its print
    if run_batch_analysis.__doc__ is None and "Warning: run_batch_analysis" in getattr(run_batch_analysis, '__code__', {}).co_consts:
        tooltip_text += " (Note: Batch analysis script is currently a placeholder)."
        status_idle_text += " (Batch AI Analysis script not fully imported)"

    return ft.Column(
        [
            ft.Text("Run AI Analysis", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM, weight=ft.FontWeight.BOLD),
            ft.Text(db_summary_text),
            ft.FilledButton(
                "Start Theme Analysis",
                icon="play_arrow",
                on_click=run_analysis_click,
                tooltip=tooltip_text
            ),
            ft.ProgressBar(ref=progress_bar_ref, value=0, width=400),
            ft.Text(ref=status_text_ref, value=status_idle_text, size=14),
        ],
        spacing=20,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        alignment=ft.MainAxisAlignment.CENTER,
        expand=True
    ) 