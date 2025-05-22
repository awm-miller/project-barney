import sqlite3
import subprocess
import argparse
import os
import json
from datetime import datetime
from dotenv import load_dotenv
import sys
import time # Added for timing and managing workers
import concurrent.futures # Added for parallel processing
import re # For SRT parsing
from pathlib import Path

# Ensure stdout and stderr use UTF-8
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

# Load environment variables from .env file
load_dotenv()

DEFAULT_DB_NAME = "pipeline_database.db"
DATABASES_DIR_NAME = "databases" # Added for consistency
DEFAULT_SUBTITLE_DIR = "subtitles"
DEFAULT_PLAINTEXT_SUBTITLE_DIR = "subtitles_plaintext" # New directory for TXT files


def create_connection(db_file): # Removed default here, path will be fully resolved before calling
    """ Create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row # Access columns by name
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
    return conn

def get_videos_to_fetch_subtitles(conn, limit=None, job_name=None):
    """Fetches videos pending subtitle check OR those with previous errors/unavailability."""
    cursor = conn.cursor()
    table_name = "videos" # Always use the main videos table

    sql = f"""
    SELECT id, video_id, video_url, title
    FROM {table_name}
    WHERE subtitle_status = 'pending_check' OR subtitle_status = 'error' OR subtitle_status = 'unavailable'
    ORDER BY id ASC
    """
    params = []
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    
    try:
        cursor.execute(sql, params)
        videos = cursor.fetchall()
        print(f"Found {len(videos)} videos to check for subtitles in table '{table_name}'.")
        return videos
    except sqlite3.Error as e:
        print(f"Database error fetching videos for subtitle check from '{table_name}': {e}")
        return []

def update_video_subtitle_status(conn, 
                                 video_db_id: int, 
                                 worker_result: dict, 
                                 job_name: str = None):
    """
    Updates the subtitle status and related fields for a video, 
    including plain text conversion fields.
    
    worker_result should contain:
    - srt_status (fetched, unavailable, error)
    - srt_file_path (path or None)
    - srt_error_message (error message or None)
    - text_source (e.g., "SUBTITLE")
    - subtitle_to_text_status (completed, failed, skipped)
    - plain_text_subtitle_path (path or None)
    - subtitle_to_text_error_message (error message or None)
    """
    cursor = conn.cursor()
    table_name = "videos" # Always use the main videos table

    # Start building the SQL SET clauses and parameters
    set_clauses = []
    parameters = []
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- SRT Status Updates ---
    set_clauses.append("subtitle_status = ?")
    parameters.append(worker_result["srt_status"])

    # Different fields to update based on SRT status
    if worker_result["srt_status"] == 'fetched':
        # SRT file path
        set_clauses.append("subtitle_file_path = ?")
        parameters.append(worker_result["srt_file_path"])
        # Timestamp
        set_clauses.append("subtitle_fetched_at = ?")
        parameters.append(current_time_str)
        # Clear error message
        set_clauses.append("subtitle_error_message = NULL")
        # Text source (e.g., "SUBTITLE")
        if worker_result.get("text_source"):
            set_clauses.append("text_source = ?")
            parameters.append(worker_result["text_source"])
    elif worker_result["srt_status"] == 'unavailable':
        set_clauses.append("subtitle_error_message = ?")
        parameters.append(worker_result.get("srt_error_message", "Subtitles unavailable"))
        set_clauses.append("subtitle_file_path = NULL")
    elif worker_result["srt_status"] == 'error':
        set_clauses.append("subtitle_error_message = ?")
        parameters.append(worker_result.get("srt_error_message", "Error during subtitle fetching"))
        set_clauses.append("subtitle_file_path = NULL")
    
    # --- Plain Text Conversion Updates ---
    # Only update these fields if SRT was fetched (avoid unnecessary updates)
    if worker_result["srt_status"] == 'fetched' or worker_result.get("subtitle_to_text_status") != "skipped":
        # Status
        set_clauses.append("subtitle_to_text_status = ?")
        parameters.append(worker_result.get("subtitle_to_text_status", "skipped"))
        
        # Path (if successful) - now maps to arabic_plain_text_path
        if worker_result.get("subtitle_to_text_status") == "completed" and worker_result.get("plain_text_subtitle_path"):
            set_clauses.append("arabic_plain_text_path = ?") # Changed to new column name
            parameters.append(worker_result["plain_text_subtitle_path"])
        else:
            set_clauses.append("arabic_plain_text_path = NULL") # Changed to new column name
            
        # Error message (if failed)
        if worker_result.get("subtitle_to_text_status") == "failed" and worker_result.get("subtitle_to_text_error_message"):
            set_clauses.append("subtitle_to_text_error_message = ?")
            parameters.append(worker_result["subtitle_to_text_error_message"])
        else:
            set_clauses.append("subtitle_to_text_error_message = NULL")
            
        # Timestamps
        set_clauses.append("subtitle_to_text_initiated_at = ?")
        parameters.append(current_time_str)
        
        if worker_result.get("subtitle_to_text_status") in ["completed", "failed"]:
            set_clauses.append("subtitle_to_text_completed_at = ?")
            parameters.append(current_time_str)
        else:
            set_clauses.append("subtitle_to_text_completed_at = NULL")
    
    # Always update last_updated_at
    set_clauses.append("last_updated_at = ?")
    parameters.append(current_time_str)
    
    # Build and execute the SQL query
    sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE id = ?"
    parameters.append(video_db_id)
    
    try:
        cursor.execute(sql, parameters)
        conn.commit()
        # print(f"Updated video (DB ID: {video_db_id}, Table: {table_name}) with subtitle and plain text status")
    except sqlite3.Error as e:
        conn.rollback()
        print(f"Database error updating video (DB ID: {video_db_id}, Table: {table_name}) subtitle/plain text status: {e}")

def convert_srt_to_plain_text(srt_file_path: str, txt_file_path: str) -> dict:
    """
    Converts an SRT subtitle file to a plain text file, stripping sequence numbers and timestamps.
    Returns a dictionary with status ('completed', 'failed') and message (error message or success).
    """
    result = {
        "status": "failed",
        "message": "",
        "txt_path": None
    }
    try:
        if not os.path.exists(srt_file_path):
            result["message"] = f"SRT file not found: {srt_file_path}"
            return result

        plain_text_lines = []
        with open(srt_file_path, 'r', encoding='utf-8') as f_srt:
            for line in f_srt:
                line = line.strip()
                if not line:  # Skip empty lines
                    continue
                if re.match(r"^\d+$", line):  # Skip sequence numbers
                    continue
                if re.match(r"^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}$", line):  # Skip timestamps
                    continue
                plain_text_lines.append(line)
        
        # Ensure the directory for the TXT file exists
        os.makedirs(os.path.dirname(txt_file_path), exist_ok=True)

        with open(txt_file_path, 'w', encoding='utf-8') as f_txt:
            f_txt.write("\n".join(plain_text_lines))
        
        result["status"] = "completed"
        result["message"] = f"Successfully converted {srt_file_path} to {txt_file_path}"
        result["txt_path"] = txt_file_path
        # print(f"[SRT->TXT] {result['message']}") # Optional: for detailed logging

    except Exception as e:
        result["message"] = f"Error converting SRT to TXT ({srt_file_path}): {e}"
        print(f"[SRT->TXT] Error: {result['message']}")
    return result

def fetch_subtitles_for_video_worker(
    video_db_id: int, video_id: str, video_url: str, video_title: str, 
    base_subtitle_dir: str, base_plaintext_subtitle_dir: str, db_name: str
):
    """
    Worker function to download subtitles and convert to plain text.
    Saves files into db_name specific subdirectories.
    - Prioritizes English subtitles for fetching
    - Only Arabic subtitles are converted to plaintext
    - Keeps both English and Arabic fixed subtitle files
    - Deletes any unfixed subtitle files after processing
    """
    start_time = time.time()
    # Create DB-specific paths
    db_name_safe = re.sub(r'[^a-zA-Z0-9_-]', '_', db_name) # Sanitize db_name for directory creation
    subtitle_dir = os.path.join(base_subtitle_dir, db_name_safe)
    plaintext_subtitle_dir = os.path.join(base_plaintext_subtitle_dir, db_name_safe)

    print(f"[Worker {video_id} DB: {db_name_safe}] Starting subtitle fetch for '{video_title}' to '{subtitle_dir}'")

    os.makedirs(subtitle_dir, exist_ok=True)
    os.makedirs(plaintext_subtitle_dir, exist_ok=True)
    
    output_template = os.path.join(subtitle_dir, f"{video_id}.%(ext)s") 
    
    cmd = [
        'yt-dlp',
        '--write-subs',
        '--write-auto-subs',
        '--convert-subs', 'srt',
        '--sub-langs', 'en,ar',  # Prioritize English (en) over Arabic (ar)
        '--skip-download',
        '--output', output_template,
        '--no-overwrites',
        '--use-postprocessor', 'srt_fix:when=before_dl',
        video_url
    ]

    # Initialize comprehensive result structure
    worker_result = {
        "video_db_id": video_db_id,
        "video_id": video_id,
        "video_title": video_title,
        "srt_status": "pending",
        "srt_file_path": None,
        "srt_error_message": None,
        "text_source": None,
        "subtitle_to_text_status": "pending",
        "plain_text_subtitle_path": None,
        "subtitle_to_text_error_message": None,
        "success": False,
        "language": None,  # Track which language was used for the primary subtitle
        "ar_file": None    # Track Arabic file specifically for text conversion
    }

    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        stdout, stderr = process.communicate(timeout=300)

        # Dictionary to keep track of all subtitle files we find (will clean up later)
        subtitle_files_found = {
            "fixed_en": None,   # priority 1 - English fixed (for primary storage)
            "fixed_ar": None,   # priority 2 - Arabic fixed (also keep this for text conversion)
            "normal_en": None,  # priority 3 - English normal
            "normal_ar": None,  # priority 4 - Arabic normal
            "vtt_en": None,     # priority 5 - English vtt
            "vtt_ar": None,     # priority 6 - Arabic vtt
            "other": []         # any other subtitle-like files
        }

        if process.returncode == 0:
            # First scan: find all subtitle files for this video
            for f_name in os.listdir(subtitle_dir):
                full_path = os.path.join(subtitle_dir, f_name)
                if not f_name.startswith(video_id):
                    continue  # Skip files not related to this video
                
                # Classify file based on name pattern
                if f_name.endswith('.en-fixed.srt'):
                    subtitle_files_found["fixed_en"] = full_path
                elif f_name.endswith('.ar-fixed.srt'):
                    subtitle_files_found["fixed_ar"] = full_path
                elif f_name.endswith('.en.srt'):
                    subtitle_files_found["normal_en"] = full_path
                elif f_name.endswith('.ar.srt'):
                    subtitle_files_found["normal_ar"] = full_path
                elif f_name.endswith('.en.vtt'):
                    subtitle_files_found["vtt_en"] = full_path
                elif f_name.endswith('.ar.vtt'):
                    subtitle_files_found["vtt_ar"] = full_path
                elif f_name.endswith('.srt') or f_name.endswith('.vtt'):
                    subtitle_files_found["other"].append(full_path)

            # Determine best English subtitle file by priority for primary storage
            best_subtitle = None
            language = None
            
            # For primary subtitle, prioritize English fixed, etc.
            if subtitle_files_found["fixed_en"]:
                best_subtitle = subtitle_files_found["fixed_en"]
                language = "en"
            elif subtitle_files_found["fixed_ar"]:
                best_subtitle = subtitle_files_found["fixed_ar"]
                language = "ar"
            elif subtitle_files_found["normal_en"]:
                best_subtitle = subtitle_files_found["normal_en"]
                language = "en"
            elif subtitle_files_found["normal_ar"]:
                best_subtitle = subtitle_files_found["normal_ar"]
                language = "ar"
            elif subtitle_files_found["vtt_en"]:
                best_subtitle = subtitle_files_found["vtt_en"]
                language = "en"
            elif subtitle_files_found["vtt_ar"]:
                best_subtitle = subtitle_files_found["vtt_ar"]
                language = "ar"
            elif subtitle_files_found["other"]:
                best_subtitle = subtitle_files_found["other"][0]

            # Find Arabic subtitle specifically for text conversion
            arabic_subtitle = subtitle_files_found["fixed_ar"] or subtitle_files_found["normal_ar"] or subtitle_files_found["vtt_ar"]
            worker_result["ar_file"] = arabic_subtitle

            if best_subtitle:
                worker_result["srt_status"] = "fetched"
                worker_result["srt_file_path"] = best_subtitle
                worker_result["text_source"] = "SUBTITLE"
                worker_result["language"] = language
                print(f"[Worker {video_id} DB: {db_name_safe}] Successfully fetched SRT: {best_subtitle} (Language: {language or 'unknown'})")

                # Remember to keep both English and Arabic fixed subtitles
                files_to_keep = [best_subtitle]
                if subtitle_files_found["fixed_ar"] and subtitle_files_found["fixed_ar"] != best_subtitle:
                    files_to_keep.append(subtitle_files_found["fixed_ar"])
                
                # Only convert if we have an Arabic subtitle
                if arabic_subtitle:
                    # Convert Arabic subtitle to plain text
                    txt_filename = f"{video_id}.txt"
                    txt_output_path = os.path.join(plaintext_subtitle_dir, txt_filename)
                    conversion_result = convert_srt_to_plain_text(arabic_subtitle, txt_output_path)
                    
                    worker_result["subtitle_to_text_status"] = conversion_result["status"]
                    if conversion_result["status"] == "completed":
                        worker_result["plain_text_subtitle_path"] = conversion_result["txt_path"]
                        worker_result["success"] = True  # Overall success
                        print(f"[Worker {video_id} DB: {db_name_safe}] Successfully converted Arabic subtitle to TXT: {conversion_result['txt_path']}")
                    else:
                        worker_result["subtitle_to_text_error_message"] = conversion_result["message"]
                        print(f"[Worker {video_id} DB: {db_name_safe}] Failed to convert Arabic SRT to TXT: {conversion_result['message']}")
                else:
                    # No Arabic subtitle available for conversion
                    worker_result["subtitle_to_text_status"] = "skipped"
                    print(f"[Worker {video_id} DB: {db_name_safe}] No Arabic subtitle available for text conversion")
                    
                    # Still mark as successful if we have an English subtitle
                    if language == "en":
                        worker_result["success"] = True

                # Clean up: Delete all subtitle files except the ones we want to keep
                deleted_count = 0
                for category, path in subtitle_files_found.items():
                    if category == "other":
                        for other_path in path:
                            if other_path not in files_to_keep and os.path.exists(other_path):
                                os.remove(other_path)
                                deleted_count += 1
                    elif path and path not in files_to_keep and os.path.exists(path):
                        os.remove(path)
                        deleted_count += 1
                
                if deleted_count > 0:
                    print(f"[Worker {video_id} DB: {db_name_safe}] Cleaned up {deleted_count} unnecessary subtitle files")
            else:
                # yt-dlp ran but no subtitle files detected by our checks
                if "has no auto captions" in stdout or "has no subtitles" in stdout or \
                   "has no auto captions" in stderr or "has no subtitles" in stderr:
                    print(f"[Worker {video_id} DB: {db_name_safe}] No subtitles available (manual or auto). yt-dlp output indicates none.")
                    worker_result["srt_status"] = "unavailable"
                    worker_result["srt_error_message"] = "No subtitles found by yt-dlp (output indicates none)."
                else:
                    print(f"[Worker {video_id} DB: {db_name_safe}] yt-dlp ran but no SRT files detected. Stdout: {stdout}, Stderr: {stderr}")
                    worker_result["srt_status"] = "error"
                    worker_result["srt_error_message"] = "yt-dlp ran but no SRT files detected by script. Check logs or subtitle_dir."
                worker_result["subtitle_to_text_status"] = "skipped"
        
        else: # yt-dlp failed with non-zero exit code
            error_message_yt_dlp = f"yt-dlp failed with exit code {process.returncode}. Stderr: {stderr.strip()}. Stdout: {stdout.strip()}"
            print(f"[Worker {video_id} DB: {db_name_safe}] {error_message_yt_dlp}")
            if "subtitles not available" in stderr.lower() or "no subtitles found" in stderr.lower():
                 print(f"[Worker {video_id} DB: {db_name_safe}] No subtitles available (yt-dlp error indicated).")
                 worker_result["srt_status"] = "unavailable"
                 worker_result["srt_error_message"] = "No subtitles found by yt-dlp (indicated by error)."
            else:
                worker_result["srt_status"] = "error"
                worker_result["srt_error_message"] = error_message_yt_dlp
            worker_result["subtitle_to_text_status"] = "skipped"

    except subprocess.TimeoutExpired:
        print(f"[Worker {video_id} DB: {db_name_safe}] Timeout while fetching subtitles.")
        worker_result["srt_status"] = "error"
        worker_result["srt_error_message"] = "yt-dlp command timed out."
        worker_result["subtitle_to_text_status"] = "skipped"
    except Exception as e:
        print(f"[Worker {video_id} DB: {db_name_safe}] An exception occurred during subtitle fetch/conversion: {e}")
        worker_result["srt_status"] = "error"
        worker_result["srt_error_message"] = f"Unhandled exception: {str(e)}"
        worker_result["subtitle_to_text_status"] = "skipped"
    
    elapsed_time = time.time() - start_time
    print(f"[Worker {video_id} DB: {db_name_safe}] Finished processing in {elapsed_time:.2f}s. Overall Success: {worker_result['success']}, "
          f"SRT: {worker_result['srt_status']}, Primary Language: {worker_result.get('language', 'None')}, "
          f"TXT: {worker_result['subtitle_to_text_status']}, Has Arabic: {worker_result['ar_file'] is not None}")
    return worker_result

def main():
    print(f"--- Starting {Path(__file__).name} --- ")
    parser = argparse.ArgumentParser(description="Fetch subtitles for YouTube videos in parallel and convert to plain text.")
    parser.add_argument("--limit", type=int, help="Maximum number of videos to process.")
    parser.add_argument("--subtitle-dir", type=str, default=DEFAULT_SUBTITLE_DIR,
                        help=f"Directory to save downloaded subtitles. Defaults to ./{DEFAULT_SUBTITLE_DIR}")
    parser.add_argument("--plaintext-subtitle-dir", type=str, default=DEFAULT_PLAINTEXT_SUBTITLE_DIR,
                        help=f"Directory to save plain text subtitle files. Defaults to ./{DEFAULT_PLAINTEXT_SUBTITLE_DIR}")
    parser.add_argument("--job-name", type=str, default=None,
                        help="Optional job name to operate on a specific table (e.g., videos_my_job).")
    parser.add_argument("--workers", type=int, default=2,
                        help="Number of parallel workers for fetching subtitles. Default: 2")
    parser.add_argument(
        "--db-name", 
        default=os.path.basename(DEFAULT_DB_NAME),
        help=f"Name of the SQLite database file to use (e.g., test.db). Expected in '{DATABASES_DIR_NAME}/' directory. Default: {os.path.basename(DEFAULT_DB_NAME)}"
    )

    args = parser.parse_args()

    if args.workers <= 0:
        print("Error: Number of workers must be a positive integer.")
        return

    # Construct the full path to the database file
    db_filename = args.db_name
    if not db_filename.endswith(".db"):
        db_filename += ".db"

    # Determine project root and construct path to databases directory
    # Assumes fetch_subtitles.py is in src/pipeline_scripts/
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_script_dir, "..", "..")) # Up to project-barney root
    databases_dir_path = os.path.join(project_root, DATABASES_DIR_NAME)
    actual_db_path = os.path.join(databases_dir_path, db_filename)

    if not os.path.exists(actual_db_path):
        print(f"Error: Database file not found at {actual_db_path}")
        print(f"Please ensure the database '{db_filename}' exists in the '{databases_dir_path}' directory.")
        print(f"You might need to run the script that creates and initializes it first (e.g., create_custom_db.py).")
        return

    # Ensure subtitle_dir and plaintext_subtitle_dir (relative to project root or absolute)
    subtitle_dir_path = args.subtitle_dir
    if not os.path.isabs(subtitle_dir_path):
        subtitle_dir_path = os.path.join(project_root, subtitle_dir_path)

    plaintext_subtitle_dir_path = args.plaintext_subtitle_dir
    if not os.path.isabs(plaintext_subtitle_dir_path):
        plaintext_subtitle_dir_path = os.path.join(project_root, plaintext_subtitle_dir_path)

    # Create both directories if they don't exist
    try:
        if not os.path.exists(subtitle_dir_path):
            os.makedirs(subtitle_dir_path)
            print(f"Created subtitle directory: {subtitle_dir_path}")
        else:
            print(f"Subtitle directory already exists: {subtitle_dir_path}")
        
        if not os.path.exists(plaintext_subtitle_dir_path):
            os.makedirs(plaintext_subtitle_dir_path)
            print(f"Created plain text subtitle directory: {plaintext_subtitle_dir_path}")
        else:
            print(f"Plain text subtitle directory already exists: {plaintext_subtitle_dir_path}")
    except OSError as e:
        print(f"Error: Could not create necessary directories: {e}")
        return

    conn = create_connection(actual_db_path)
    if not conn:
        print(f"Failed to connect to the database '{actual_db_path}'. Exiting.")
        return

    videos_to_process_initial_list = get_videos_to_fetch_subtitles(conn, args.limit, args.job_name)

    if not videos_to_process_initial_list:
        print("[Main] No videos found needing subtitle fetching at this time.")
        if conn: conn.close()
        return
        
    total_videos_to_attempt = len(videos_to_process_initial_list)
    print(f"[Main] Initializing subtitle fetching and text conversion for {total_videos_to_attempt} videos with {args.workers} workers...")
    print("[Main] NOTE: Prioritizing English subtitles for storage, but converting ONLY Arabic subtitles to plaintext")
    print("[Main] All unfixed subtitle files will be deleted after processing, keeping only fixed English and Arabic SRTs")

    # Counters for various statuses
    processed_count = 0
    success_count = 0
    unavailable_count = 0
    error_count = 0
    
    # Language-specific counters
    en_count = 0       # English primary subtitles
    ar_primary_count = 0  # Arabic as primary subtitle (when English not available)
    ar_count = 0       # Arabic subtitle files found (regardless if primary)
    txt_ar_count = 0   # Arabic plaintext conversions
    
    start_time_main = time.time()

    failed_first_pass_video_rows = [] 

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        print("[Main] --- Starting Initial Subtitle Fetch & Text Conversion Pass ---")
        db_name_for_worker = Path(actual_db_path).stem # Get DB name without .db extension
        future_to_video_details = {
            executor.submit(
                fetch_subtitles_for_video_worker, 
                video_row['id'], 
                video_row['video_id'], 
                video_row['video_url'],
                video_row['title'], 
                subtitle_dir_path, # base directory
                plaintext_subtitle_dir_path, # base directory
                db_name_for_worker # Pass the database name
            ): video_row 
            for video_row in videos_to_process_initial_list
        }

        for future in concurrent.futures.as_completed(future_to_video_details):
            original_video_row = future_to_video_details[future]
            video_id_log = original_video_row['video_id']
            video_db_id_original = original_video_row['id']

            try:
                result = future.result()
                processed_count += 1
                
                video_db_id_res = result["video_db_id"]
                srt_status_res = result["srt_status"]
                txt_status_res = result.get("subtitle_to_text_status", "skipped")
                primary_language = result.get("language")
                has_arabic = result.get("ar_file") is not None
                
                # Track language stats
                if primary_language == "en":
                    en_count += 1
                elif primary_language == "ar":
                    ar_primary_count += 1
                
                if has_arabic:
                    ar_count += 1
                    if txt_status_res == "completed":
                        txt_ar_count += 1
                
                # Determine overall status for logging
                if result["success"]:  # Has subtitles and if Arabic available, converted to text
                    success_count += 1
                    if has_arabic and txt_status_res == "completed":
                        print(f"[Main] DB Update (Initial Pass): Video {video_id_log} - Primary: {primary_language}, "
                              f"Has Arabic: Yes, TXT: '{txt_status_res}', Path: {result.get('plain_text_subtitle_path')}")
                    else:
                        print(f"[Main] DB Update (Initial Pass): Video {video_id_log} - Primary: {primary_language}, "
                              f"Has Arabic: {has_arabic}, TXT: '{txt_status_res}'")
                    update_video_subtitle_status(conn, video_db_id_res, result, args.job_name)
                elif srt_status_res == 'fetched':  # SRT fetched but Arabic text conversion failed or not available
                    if has_arabic and txt_status_res == "failed":
                        print(f"[Main] DB Update (Initial Pass): Video {video_id_log} - Primary: {primary_language}, "
                              f"Has Arabic but text conversion failed: {result.get('subtitle_to_text_error_message')}")
                    else:
                        print(f"[Main] DB Update (Initial Pass): Video {video_id_log} - Primary: {primary_language}, "
                              f"Has Arabic: {has_arabic}, TXT: '{txt_status_res}'")
                    update_video_subtitle_status(conn, video_db_id_res, result, args.job_name)
                elif srt_status_res == 'unavailable':
                    unavailable_count += 1
                    print(f"[Main] DB Update (Initial Pass): Video {video_id_log} - SRT: '{srt_status_res}' (Unavailable). Reason: {result.get('srt_error_message')}")
                    update_video_subtitle_status(conn, video_db_id_res, result, args.job_name)
                elif srt_status_res == 'error':
                    error_count += 1
                    failed_first_pass_video_rows.append(original_video_row) 
                    print(f"[Main] DB Update (Initial Pass): Video {video_id_log} - SRT: '{srt_status_res}'. Error: {result.get('srt_error_message')}. Queued for retry.")
                    update_video_subtitle_status(conn, video_db_id_res, result, args.job_name)
                else:
                    # Unknown/unexpected status
                    error_count += 1 
                    failed_first_pass_video_rows.append(original_video_row) 
                    err_msg = f"Unknown status combination - SRT: '{srt_status_res}', TXT: '{txt_status_res}'"
                    print(f"[Main] DB Update (Initial Pass): Video {video_id_log} - {err_msg}. Marking as error and queuing for retry.")
                    # Create a simpler result dict for the unknown case
                    simple_error_result = {'srt_status': 'error', 'srt_error_message': err_msg}
                    update_video_subtitle_status(conn, video_db_id_res, simple_error_result, args.job_name)

            except Exception as exc:
                processed_count += 1
                error_count += 1
                failed_first_pass_video_rows.append(original_video_row) 
                err_msg = f"Exception processing result: {exc}"
                print(f"[Main] Exception (Initial Pass) for video {video_id_log}: {err_msg}. Queued for retry.")
                simple_error_result = {'srt_status': 'error', 'srt_error_message': err_msg}
                update_video_subtitle_status(conn, video_db_id_original, simple_error_result, args.job_name)
            
            if processed_count % 10 == 0 or processed_count == total_videos_to_attempt:
                 elapsed_main = time.time() - start_time_main
                 print(f"[Main] Progress (Initial Pass): {processed_count}/{total_videos_to_attempt} processed. "
                       f"Success: {success_count}, Unavailable: {unavailable_count}, Errors: {error_count}, "
                       f"English Primary: {en_count}, Arabic Primary: {ar_primary_count}, Arabic Found: {ar_count}, "
                       f"Arabic TXT: {txt_ar_count}. Elapsed: {elapsed_main:.2f}s")

        # --- Retry Pass Logic ---
        if failed_first_pass_video_rows:
            print(f"[Main] --- Starting Retry Pass for {len(failed_first_pass_video_rows)} Failed Videos ---")
            retried_success_count = 0
            retried_unavailable_count = 0
            retried_final_error_count = 0
            
            # Language-specific retry counters
            retried_en_count = 0
            retried_ar_primary_count = 0
            retried_ar_count = 0
            retried_txt_ar_count = 0
            
            # Reset error_count for retry pass counting
            error_count = 0 

            future_to_video_details_retry = {
                executor.submit(
                    fetch_subtitles_for_video_worker,
                    video_row['id'],
                    video_row['video_id'],
                    video_row['video_url'],
                    video_row['title'],
                    subtitle_dir_path, # base directory
                    plaintext_subtitle_dir_path, # base directory
                    db_name_for_worker # Pass the database name for retry pass too
                ): video_row for video_row in failed_first_pass_video_rows
            }

            retry_processed_count = 0
            for future in concurrent.futures.as_completed(future_to_video_details_retry):
                original_video_row_retry = future_to_video_details_retry[future]
                video_id_log_retry = original_video_row_retry['video_id']
                video_db_id_original_retry = original_video_row_retry['id']
                retry_processed_count += 1

                try:
                    result_retry = future.result()
                    video_db_id_res_retry = result_retry["video_db_id"]
                    srt_status_res_retry = result_retry["srt_status"]
                    txt_status_res_retry = result_retry.get("subtitle_to_text_status", "skipped")
                    primary_language_retry = result_retry.get("language")
                    has_arabic_retry = result_retry.get("ar_file") is not None
                    
                    # Track language stats for retry
                    if primary_language_retry == "en":
                        retried_en_count += 1
                        en_count += 1
                    elif primary_language_retry == "ar":
                        retried_ar_primary_count += 1
                        ar_primary_count += 1
                    
                    if has_arabic_retry:
                        retried_ar_count += 1
                        ar_count += 1
                        if txt_status_res_retry == "completed":
                            retried_txt_ar_count += 1
                            txt_ar_count += 1

                    if result_retry["success"]:  # Has subtitles and if Arabic available, converted to text
                        success_count += 1 
                        retried_success_count += 1
                        if has_arabic_retry and txt_status_res_retry == "completed":
                            print(f"[Main] DB Update (Retry Pass): Video {video_id_log_retry} - Primary: {primary_language_retry}, "
                                 f"Has Arabic: Yes, TXT: '{txt_status_res_retry}', Path: {result_retry.get('plain_text_subtitle_path')}")
                        else:
                            print(f"[Main] DB Update (Retry Pass): Video {video_id_log_retry} - Primary: {primary_language_retry}, "
                                 f"Has Arabic: {has_arabic_retry}, TXT: '{txt_status_res_retry}'")
                        update_video_subtitle_status(conn, video_db_id_res_retry, result_retry, args.job_name)
                    elif srt_status_res_retry == 'fetched':  # SRT fetched but Arabic text conversion failed or not available
                        if has_arabic_retry and txt_status_res_retry == "failed":
                            print(f"[Main] DB Update (Retry Pass): Video {video_id_log_retry} - Primary: {primary_language_retry}, "
                                 f"Has Arabic but text conversion failed: {result_retry.get('subtitle_to_text_error_message')}")
                        else:
                            print(f"[Main] DB Update (Retry Pass): Video {video_id_log_retry} - Primary: {primary_language_retry}, "
                                 f"Has Arabic: {has_arabic_retry}, TXT: '{txt_status_res_retry}'")
                        update_video_subtitle_status(conn, video_db_id_res_retry, result_retry, args.job_name)
                    elif srt_status_res_retry == 'unavailable':
                        unavailable_count += 1 
                        retried_unavailable_count += 1
                        print(f"[Main] DB Update (Retry Pass): Video {video_id_log_retry} - SRT: '{srt_status_res_retry}' (Unavailable). Reason: {result_retry.get('srt_error_message')}")
                        update_video_subtitle_status(conn, video_db_id_res_retry, result_retry, args.job_name)
                    elif srt_status_res_retry == 'error':
                        error_count += 1 
                        retried_final_error_count += 1
                        print(f"[Main] DB Update (Retry Pass): Video {video_id_log_retry} - SRT: '{srt_status_res_retry}'. Error: {result_retry.get('srt_error_message')} (final after retry).")
                        update_video_subtitle_status(conn, video_db_id_res_retry, result_retry, args.job_name)
                    else: 
                        error_count += 1 
                        retried_final_error_count += 1
                        err_msg = f"Unknown status combination on retry - SRT: '{srt_status_res_retry}', TXT: '{txt_status_res_retry}'"
                        print(f"[Main] DB Update (Retry Pass): Video {video_id_log_retry} - {err_msg}. Marking as error.")
                        simple_error_result_retry = {'srt_status': 'error', 'srt_error_message': err_msg}
                        update_video_subtitle_status(conn, video_db_id_res_retry, simple_error_result_retry, args.job_name)

                except Exception as exc_retry:
                    error_count += 1
                    retried_final_error_count += 1
                    err_msg = f"Exception on retry: {exc_retry}"
                    print(f"[Main] Exception (Retry Pass) for video {video_id_log_retry}: {err_msg}")
                    simple_error_result_retry = {'srt_status': 'error', 'srt_error_message': err_msg}
                    update_video_subtitle_status(conn, video_db_id_original_retry, simple_error_result_retry, args.job_name)

                if retry_processed_count % 10 == 0 or retry_processed_count == len(failed_first_pass_video_rows):
                    elapsed_main = time.time() - start_time_main
                    print(f"[Main] Progress (Retry Pass): {retry_processed_count}/{len(failed_first_pass_video_rows)} retries processed. "
                          f"Success on Retry: {retried_success_count}, Unavailable on Retry: {retried_unavailable_count}, "
                          f"Final Errors after Retry: {retried_final_error_count}, "
                          f"English Primary: +{retried_en_count}, Arabic Primary: +{retried_ar_primary_count}, "
                          f"Arabic Found: +{retried_ar_count}, Arabic TXT: +{retried_txt_ar_count}. "
                          f"Total Elapsed: {elapsed_main:.2f}s")
        else:
            print("[Main] --- No Videos Failed in Initial Pass. Skipping Retry Pass. ---")

    print(f"--- Subtitle Fetching and Text Conversion Complete (Including Retries) ---")
    total_time_main = time.time() - start_time_main
    
    # Final summary with language statistics
    print(f"Attempted to process {total_videos_to_attempt} videos in {total_time_main:.2f} seconds.")
    print(f"Summary: Successful SRT Fetch: {success_count}, Unavailable: {unavailable_count}, Errors: {error_count}")
    print(f"Language Stats: English Primary: {en_count}, Arabic Primary: {ar_primary_count}")
    print(f"Arabic Subtitles Found: {ar_count}, Arabic Plaintext Generated: {txt_ar_count}")
    
    if conn:
        conn.close()
        print("[Main] Database connection closed.")

if __name__ == '__main__':
    main() 