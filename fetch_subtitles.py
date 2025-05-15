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

# Load environment variables from .env file
load_dotenv()

DATABASE_NAME = "pipeline_database.db"
# DEFAULT_SUBTITLE_DIR = os.getenv("SUBTITLE_DIR", "subtitles") # We'll need to define SUBTITLE_DIR in .env or use a default
DEFAULT_SUBTITLE_DIR = "subtitles"


def create_connection(db_file=DATABASE_NAME):
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
    table_name = f"videos_{job_name}" if job_name else "videos"

    sql = f"""
    SELECT id, video_id, video_url, title
    FROM {table_name}
    WHERE subtitle_status = 'error' OR subtitle_status = 'unavailable'
    ORDER BY added_at ASC
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

def update_video_subtitle_status(conn, video_db_id: int, status: str, 
                                 file_path: str = None, error_message: str = None,
                                 text_source: str = None, job_name: str = None):
    """Updates the subtitle status and related fields for a video."""
    # Ensure thread-safety for database operations if connections are shared or use per-thread connections.
    # For SQLite with a single connection, operations should be serialized.
    # If using a connection pool, ensure it's thread-safe.
    # Python's built-in sqlite3 module connections are not thread-safe for sharing across threads if writes are concurrent.
    # For this script, if main creates one conn and workers don't directly use it, it's fine.
    # If workers were to update DB, each would need its own conn or use a queue to send updates to main.
    # Here, updates are done in main based on worker results, so existing conn is fine.

    cursor = conn.cursor()
    table_name = f"videos_{job_name}" if job_name else "videos"

    set_clauses = ["subtitle_status = ?"]
    parameters = [status]

    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if status == 'fetched':
        set_clauses.append("subtitle_file_path = ?")
        parameters.append(file_path)
        set_clauses.append("subtitle_fetched_at = ?")
        parameters.append(current_time_str)
        set_clauses.append("subtitle_error_message = NULL")
        if text_source:
            set_clauses.append("text_source = ?")
            parameters.append(text_source)
    elif status == 'unavailable':
        set_clauses.append("subtitle_error_message = NULL")
        set_clauses.append("subtitle_file_path = NULL")
    elif status == 'error':
        set_clauses.append("subtitle_error_message = ?")
        parameters.append(error_message)
        set_clauses.append("subtitle_file_path = NULL")
    
    set_clauses.append("last_updated_at = ?")
    parameters.append(current_time_str)
    
    sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE id = ?"
    parameters.append(video_db_id)
    
    try:
        cursor.execute(sql, parameters)
        conn.commit()
        # print(f"Updated video (DB ID: {video_db_id}, Table: {table_name}) subtitle_status to: '{status}'.")
    except sqlite3.Error as e:
        conn.rollback()
        print(f"Database error updating video (DB ID: {video_db_id}, Table: {table_name}) subtitle status: {e}")

def fetch_subtitles_for_video_worker(video_db_id: int, video_id: str, video_url: str, video_title: str, subtitle_dir: str):
    """
    Worker function to download subtitles for a single video.
    Returns a dictionary containing results for the main thread to process.
    """
    # This function encapsulates the previous fetch_subtitles_for_video logic
    # but is designed to be called by a thread pool executor.
    # It returns more context (like video_db_id) for the main thread.

    start_time = time.time()
    print(f"[Worker {video_id}] Starting subtitle fetch for '{video_title}'")

    # Ensure subtitle_dir exists (though main also checks, good for robustness if called directly)
    os.makedirs(subtitle_dir, exist_ok=True)
    
    output_template = os.path.join(subtitle_dir, f"{video_id}.%(ext)s") 
    
    cmd = [
        'yt-dlp',
        '--write-subs',
        '--write-auto-subs',
        '--convert-subs', 'srt',
        '--sub-langs', 'en,ar',
        '--skip-download',
        '--output', output_template,
        '--no-overwrites',
        '--use-postprocessor', 'srt_fix:when=before_dl',
        video_url
    ]

    result = {
        "video_db_id": video_db_id,
        "video_id": video_id,
        "video_title": video_title,
        "status": None, # Will be 'fetched', 'unavailable', or 'error'
        "detail": None, # File path or error message
        "success": False # Redundant with status, but can be useful
    }

    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        stdout, stderr = process.communicate(timeout=300) # 5 min timeout

        if process.returncode == 0:
            processed_subs = []
            for f_name in os.listdir(subtitle_dir):
                if f_name.startswith(video_id) and f_name.endswith('-fixed.srt'):
                    processed_subs.append(f_name)

            if processed_subs:
                subtitle_file = os.path.join(subtitle_dir, processed_subs[0])
                result["status"] = "fetched"
                result["detail"] = subtitle_file
                result["success"] = True
                print(f"[Worker {video_id}] Successfully fetched and fixed subtitle: {subtitle_file}")
            else:
                if "has no auto captions" in stdout or "has no subtitles" in stdout or \
                   "has no auto captions" in stderr or "has no subtitles" in stderr:
                    print(f"[Worker {video_id}] No subtitles available (manual or auto).")
                    result["status"] = "unavailable"
                    result["detail"] = "No subtitles found by yt-dlp."
                else:
                    print(f"[Worker {video_id}] yt-dlp ran but no subtitle files detected. Stdout: {stdout}, Stderr: {stderr}")
                    result["status"] = "error"
                    result["detail"] = "yt-dlp ran but no subtitle files detected. Check logs."
        else:
            error_message = f"yt-dlp failed with exit code {process.returncode}. Stderr: {stderr.strip()}. Stdout: {stdout.strip()}"
            print(f"[Worker {video_id}] {error_message}")
            if "subtitles not available" in stderr.lower() or "no subtitles found" in stderr.lower():
                 print(f"[Worker {video_id}] No subtitles available (yt-dlp error indicated).")
                 result["status"] = "unavailable"
                 result["detail"] = "No subtitles found by yt-dlp (indicated by error)."
            else:
                result["status"] = "error"
                result["detail"] = error_message

    except subprocess.TimeoutExpired:
        print(f"[Worker {video_id}] Timeout while fetching subtitles.")
        result["status"] = "error"
        result["detail"] = "yt-dlp command timed out."
    except Exception as e:
        print(f"[Worker {video_id}] An exception occurred: {e}")
        result["status"] = "error"
        result["detail"] = str(e)
    
    elapsed_time = time.time() - start_time
    print(f"[Worker {video_id}] Finished processing in {elapsed_time:.2f}s. Status: {result['status']}")
    return result

def main():
    parser = argparse.ArgumentParser(description="Fetch subtitles for YouTube videos in parallel.")
    parser.add_argument("--limit", type=int, help="Maximum number of videos to process.")
    parser.add_argument("--subtitle-dir", type=str, default=DEFAULT_SUBTITLE_DIR,
                        help=f"Directory to save downloaded subtitles. Defaults to ./{DEFAULT_SUBTITLE_DIR}")
    parser.add_argument("--job-name", type=str, default=None,
                        help="Optional job name to operate on a specific table (e.g., videos_my_job).")
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of parallel workers for fetching subtitles. Default: 4")

    args = parser.parse_args()

    if args.workers <= 0:
        print("Error: Number of workers must be a positive integer.")
        return

    if not os.path.exists(args.subtitle_dir):
        try:
            os.makedirs(args.subtitle_dir)
            print(f"Created subtitle directory: {args.subtitle_dir}")
        except OSError as e:
            print(f"Error: Could not create subtitle directory {args.subtitle_dir}: {e}")
            return

    conn = create_connection(DATABASE_NAME)
    if not conn:
        print("Failed to connect to the database. Exiting.")
        return

    videos_to_process_initial_list = get_videos_to_fetch_subtitles(conn, args.limit, args.job_name)

    if not videos_to_process_initial_list:
        print("[Main] No videos found needing subtitle fetching at this time.")
        if conn: conn.close()
        return
        
    total_videos_to_attempt = len(videos_to_process_initial_list)
    print(f"[Main] Initializing subtitle fetching for {total_videos_to_attempt} videos with {args.workers} workers...")

    processed_count = 0
    success_count = 0
    unavailable_count = 0
    error_count = 0
    
    start_time_main = time.time()

    failed_first_pass_video_rows = [] 

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        print("[Main] --- Starting Initial Subtitle Fetch Pass ---")
        future_to_video_details = {
            executor.submit(
                fetch_subtitles_for_video_worker, 
                video_row['id'], 
                video_row['video_id'], 
                video_row['video_url'],
                video_row['title'], 
                args.subtitle_dir
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
                status_res = result["status"]
                detail_res = result["detail"]
        
                current_text_source = None
                if status_res == 'fetched':
                    success_count +=1
                    current_text_source = 'SUBTITLE'
                    update_video_subtitle_status(conn, video_db_id_res, status_res, file_path=detail_res, text_source=current_text_source, job_name=args.job_name)
                    print(f"[Main] DB Update (Initial Pass): Video {video_id_log} status to '{status_res}', path: {detail_res}")
                elif status_res == 'unavailable':
                    unavailable_count += 1
                    update_video_subtitle_status(conn, video_db_id_res, status_res, error_message=detail_res, job_name=args.job_name)
                    print(f"[Main] DB Update (Initial Pass): Video {video_id_log} status to '{status_res}'. Reason: {detail_res}")
                elif status_res == 'error':
                    error_count += 1
                    failed_first_pass_video_rows.append(original_video_row) 
                    update_video_subtitle_status(conn, video_db_id_res, status_res, error_message=detail_res, job_name=args.job_name)
                    print(f"[Main] DB Update (Initial Pass): Video {video_id_log} status to '{status_res}'. Error: {detail_res}. Queued for retry.")
                else:
                    error_count += 1 
                    failed_first_pass_video_rows.append(original_video_row) 
                    print(f"[Main] UNKNOWN status (Initial Pass) '{status_res}' for video {video_id_log}. Detail: {detail_res}. Marking as error and queuing for retry.")
                    update_video_subtitle_status(conn, video_db_id_res, 'error', error_message=f"Unknown status: {status_res}. Detail: {detail_res}", job_name=args.job_name)

            except Exception as exc:
                processed_count += 1
                error_count += 1
                failed_first_pass_video_rows.append(original_video_row) 
                print(f"[Main] Exception (Initial Pass) processing result for video {video_id_log}: {exc}. Queued for retry.")
                update_video_subtitle_status(conn, video_db_id_original, 'error', error_message=str(exc), job_name=args.job_name)
            
            if processed_count % 10 == 0 or processed_count == total_videos_to_attempt:
                 elapsed_main = time.time() - start_time_main
                 print(f"[Main] Progress (Initial Pass): {processed_count}/{total_videos_to_attempt} processed. "
                       f"Success: {success_count}, Unavailable: {unavailable_count}, Errors (pre-retry): {error_count}. "
                       f"Elapsed: {elapsed_main:.2f}s")

        if failed_first_pass_video_rows:
            print(f"[Main] --- Starting Retry Pass for {len(failed_first_pass_video_rows)} Failed Videos ---")
            retried_success_count = 0
            retried_unavailable_count = 0
            retried_final_error_count = 0
            
            error_count = 0 

            future_to_video_details_retry = {
                executor.submit(
                    fetch_subtitles_for_video_worker,
                    video_row['id'],
                    video_row['video_id'],
                    video_row['video_url'],
                    video_row['title'],
                    args.subtitle_dir
                ): video_row for video_row in failed_first_pass_video_rows
            }

            retry_processed_count = 0
            for future in concurrent.futures.as_completed(future_to_video_details_retry):
                original_video_row_retry = future_to_video_details_retry[future]
                video_id_log_retry = original_video_row_retry['video_id']
                video_db_id_original_retry = original_video_row_retry['id']
                retry_processed_count +=1

                try:
                    result_retry = future.result()
                    video_db_id_res_retry = result_retry["video_db_id"]
                    status_res_retry = result_retry["status"]
                    detail_res_retry = result_retry["detail"]

                    current_text_source_retry = None
                    if status_res_retry == 'fetched':
                        success_count += 1 
                        retried_success_count += 1
                        current_text_source_retry = 'SUBTITLE'
                        update_video_subtitle_status(conn, video_db_id_res_retry, status_res_retry, file_path=detail_res_retry, text_source=current_text_source_retry, job_name=args.job_name)
                        print(f"[Main] DB Update (Retry Pass): Video {video_id_log_retry} status to '{status_res_retry}', path: {detail_res_retry}")
                    elif status_res_retry == 'unavailable':
                        unavailable_count += 1 
                        retried_unavailable_count += 1
                        update_video_subtitle_status(conn, video_db_id_res_retry, status_res_retry, error_message=detail_res_retry, job_name=args.job_name)
                        print(f"[Main] DB Update (Retry Pass): Video {video_id_log_retry} status to '{status_res_retry}'. Reason: {detail_res_retry}")
                    elif status_res_retry == 'error':
                        error_count += 1 
                        retried_final_error_count += 1
                        update_video_subtitle_status(conn, video_db_id_res_retry, status_res_retry, error_message=detail_res_retry, job_name=args.job_name)
                        print(f"[Main] DB Update (Retry Pass): Video {video_id_log_retry} status to '{status_res_retry}' (final after retry). Error: {detail_res_retry}")
                    else: 
                        error_count += 1 
                        retried_final_error_count += 1
                        print(f"[Main] UNKNOWN status (Retry Pass) '{status_res_retry}' for video {video_id_log_retry}. Detail: {detail_res_retry}. Marking as error.")
                        update_video_subtitle_status(conn, video_db_id_res_retry, 'error', error_message=f"Unknown status on retry: {status_res_retry}. Detail: {detail_res_retry}", job_name=args.job_name)

                except Exception as exc_retry:
                    error_count += 1
                    retried_final_error_count += 1
                    print(f"[Main] Exception (Retry Pass) processing result for video {video_id_log_retry}: {exc_retry}")
                    update_video_subtitle_status(conn, video_db_id_original_retry, 'error', error_message=f"Exception on retry: {str(exc_retry)}", job_name=args.job_name)

                if retry_processed_count % 10 == 0 or retry_processed_count == len(failed_first_pass_video_rows):
                    elapsed_main = time.time() - start_time_main
                    print(f"[Main] Progress (Retry Pass): {retry_processed_count}/{len(failed_first_pass_video_rows)} retries processed. "
                          f"Success on Retry: {retried_success_count}, Unavailable on Retry: {retried_unavailable_count}, Final Errors after Retry: {retried_final_error_count}. "
                          f"Total Elapsed: {elapsed_main:.2f}s")
        else:
            print("[Main] --- No Videos Failed in Initial Pass. Skipping Retry Pass. ---")

    print(f"--- Subtitle Fetching Complete (Including Retries) ---")
    total_time_main = time.time() - start_time_main
    # Using total_videos_to_attempt for clarity on how many items were initially processed.
    print(f"Attempted to process {total_videos_to_attempt} videos in {total_time_main:.2f} seconds.")
    print(f"Summary: Successful: {success_count}, Unavailable: {unavailable_count}, Errors: {error_count}")
    
    if conn:
        conn.close()
        print("[Main] Database connection closed.")

if __name__ == '__main__':
    main() 