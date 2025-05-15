import os
import logging
import argparse
import random
import time
import json
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
import yt_dlp
import concurrent.futures

# Import from our database manager
from database_manager import create_connection, DATABASE_NAME

# --- Configuration ---
load_dotenv()

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")
if not DOWNLOAD_DIR:
    raise ValueError("DOWNLOAD_DIR not found in .env file. Please set it to your desired video download path.")

LOG_FILE = "download_videos.log"
SCRIPT_NAME = "download_videos.py"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Database Helper Functions ---

def get_videos_to_download_from_db(conn, limit=None):
    """
    Fetches videos from the database that have not been successfully downloaded.
    Joins with channels to get institution_name for filename/logging.
    Orders by published_at in descending order to prioritize recent videos.
    """
    cursor = conn.cursor()
    # Select videos whose download status is not 'completed' (includes pending, failed, NEW, NULL, etc.)
    sql = f"""
    SELECT
        v.id as video_db_id,
        v.video_id as youtube_video_id,
        v.video_url,
        v.title as video_title,
        v.download_status,
        c.institution_name,
        c.channel_title
    FROM videos v
    LEFT JOIN channels c ON v.channel_id = c.channel_id
    WHERE (v.download_status IS NULL OR v.download_status != 'completed')
      AND (v.subtitle_status IS NULL OR v.subtitle_status = 'unavailable' OR v.subtitle_status = 'error')
    ORDER BY v.published_at DESC, v.added_at ASC
    """
    params = []
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    
    try:
        cursor.execute(sql, params)
        videos_data = cursor.fetchall()
        logging.info(f"Found {len(videos_data)} videos not marked as 'completed' AND where subtitles are unavailable/errored, to process from database.")
        return [dict(zip([column[0] for column in cursor.description], row)) for row in videos_data]
    except sqlite3.Error as e:
        logging.error(f"Database error fetching videos to download: {e}")
        return []

def update_video_download_details_db(conn, video_db_pk, status_str, download_path_str=None, error_msg_str=None):
    """Updates video download status, path, errors, and timestamps."""
    sql = """
    UPDATE videos
    SET download_status = ?,
        download_path = ?,
        download_error_message = ?,
        download_initiated_at = COALESCE(download_initiated_at, CASE WHEN ? IN ('downloading', 'completed', 'failed') THEN CURRENT_TIMESTAMP ELSE NULL END),
        download_completed_at = CASE WHEN ? = 'completed' THEN CURRENT_TIMESTAMP ELSE download_completed_at END,
        last_updated_at = CURRENT_TIMESTAMP
    WHERE id = ?;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (status_str, download_path_str, error_msg_str, status_str, status_str, video_db_pk))
        conn.commit()
        logging.info(f"Updated video (DB ID: {video_db_pk}) to download_status: '{status_str}'.")
    except sqlite3.Error as e:
        logging.error(f"Database error updating video (DB ID: {video_db_pk}) download details: {e}")

def add_processing_log_db(conn, video_db_pk, stage_str, status_str, message_str, details_dict=None):
    """Adds an entry to the processing_logs table."""
    details_json_str = json.dumps(details_dict) if details_dict else None
    sql = """
    INSERT INTO processing_logs (video_record_id, stage, status, message, details, timestamp, source_script)
    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?);
    """
    try:
        cursor = conn.cursor()
        if video_db_pk is not None:
             cursor.execute(sql, (video_db_pk, stage_str, status_str, message_str, details_json_str, SCRIPT_NAME))
             conn.commit()
             logging.debug(f"Logged to processing_logs for video_record_id {video_db_pk}: {stage_str} - {status_str}")
        else:
            logging.warning(f"Skipped logging to processing_logs due to null video_record_id. Message: {message_str}")
    except sqlite3.Error as e:
        logging.error(f"Database error adding processing log for video_record_id {video_db_pk}: {e}")

# --- Helper Functions ---
def sanitize_filename(name: str) -> str:
    if name is None:
        return ""
    name = name.replace(':', '_').replace('/', '_').replace('\\\\', '_').replace('?', '_').replace('*', '_')
    name = "".join(c if c.isalnum() or c in ' ._-()[]' else '_' for c in name).strip()
    return name[:150]

def download_video(video_db_id: int, yt_video_id: str, video_url: str, video_title: str | None, institution_name: str | None, current_download_dir: str):
    """
    Downloads a single video using yt-dlp.
    Returns a dictionary with download results.
    """
    start_time = time.time()
    current_institution_name_safe = sanitize_filename(institution_name if institution_name else "Unknown_Institution")
    video_title_safe = sanitize_filename(video_title if video_title else "Untitled_Video")
    fname_base = f"{current_institution_name_safe}_{video_title_safe}_{yt_video_id}"
    fname = f"{fname_base}.mp4"
    out_path = os.path.join(current_download_dir, fname)
    os.makedirs(current_download_dir, exist_ok=True)
    log_video_title = video_title if video_title else yt_video_id
    # Logging of initiation will be handled by the main thread before submitting to pool
    # logging.info(f"Starting download for video {yt_video_id} ('{log_video_title}') to {out_path}")

    result = {
        "video_db_id": video_db_id,
        "yt_video_id": yt_video_id,
        "success": False,
        "file_path": None,
        "error_message": None,
        "video_title": video_title # For logging upon completion
    }

    ydl_opts = {
        'outtmpl': out_path,
        'quiet': True, # Quieter for parallel execution, detailed logs from main thread
        'noprogress': True, # No progress bars from yt-dlp itself
        'format': 'bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/best[ext=mp4][height<=720]/best',
        'retries': 2, # Fewer retries in worker, can be retried by main script logic if status remains 'failed'
        'fragment_retries': 2,
        'socket_timeout': 60,
        'verbose': False,
        'enable_word_time_offsets': True, # Ensure this is True to get word timestamps
    }
    try:
        # logging.debug(f"[Worker {yt_video_id}] Starting yt-dlp download to {out_path}")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        # logging.debug(f"[Worker {yt_video_id}] yt-dlp download call finished.")
        
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            # logging.info(f"[Worker {yt_video_id}] Downloaded successfully to {out_path}")
            result["success"] = True
            result["file_path"] = out_path
        else:
            # logging.error(f"[Worker {yt_video_id}] File not found/empty: {out_path}")
            result["error_message"] = "File not found or empty after download attempt"

    except yt_dlp.utils.DownloadError as e:
        # logging.error(f"[Worker {yt_video_id}] yt-dlp DownloadError: {e}")
        error_message = str(e)[:250]
        if "Video unavailable" in str(e): error_message = "Video unavailable"
        elif "Private video" in str(e): error_message = "Private video"
        elif "HTTP Error 403" in str(e) or "Access denied" in str(e): error_message = "Access denied/Forbidden (403)"
        elif "Premiere will begin" in str(e): error_message = "Video is a premiere, not yet available"
        elif "live event will begin" in str(e): error_message = "Video is a live event, not yet available"
        result["error_message"] = error_message
    except Exception as e:
        # logging.error(f"[Worker {yt_video_id}] Generic download exception: {e}")
        result["error_message"] = str(e)[:250]
    
    # elapsed_time = time.time() - start_time
    # logging.debug(f"[Worker {yt_video_id}] download_video function finished in {elapsed_time:.2f}s. Success: {result['success']}")
    return result

def format_time_delta(seconds):
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds_val = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds_val:02d}"

# --- Main Execution ---
def main(effective_download_dir: str, download_limit: int | None, statuses_to_process: list[str], max_workers: int):
    total_start_time = time.time()
    logging.info(f"--- Starting Bulk Video Download Script (Parallel Rolling) at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    logging.info(f"Videos will be downloaded to: {effective_download_dir}")
    logging.info(f"Processing videos not marked as 'completed' AND where subtitles are unavailable or errored.")
    logging.info(f"Using a maximum of {max_workers} parallel workers (rolling submission).")
    if download_limit:
        logging.info(f"Will attempt to process at most {download_limit} videos this run.")

    conn = create_connection(DATABASE_NAME)
    if not conn:
        logging.error("Could not connect to the database. Exiting.")
        return

    videos_to_process_list = get_videos_to_download_from_db(conn, limit=download_limit)

    if not videos_to_process_list:
        logging.info("No videos found in database needing download. Exiting.")
        if conn: conn.close()
        return

    total_videos_to_attempt = len(videos_to_process_list)
    videos_processed_count = 0
    videos_downloaded_this_run = 0
    videos_failed_this_run = 0

    video_iterator = iter(videos_to_process_list) # To pull videos one by one

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            active_futures = set()

            while True:
                # Phase 1: Submit new tasks if there's capacity and videos are available
                while len(active_futures) < max_workers:
                    try:
                        video_data = next(video_iterator)
                    except StopIteration:
                        # No more videos to submit
                        video_data = None 
                        break # Break from submission loop, continue to process active futures
                    
                    # --- Pre-submission processing for this video_data ---
                    video_db_id = video_data['video_db_id']
                    yt_video_id = video_data['youtube_video_id']
                    video_url = video_data['video_url']
                    video_title = video_data['video_title']
                    institution_name = video_data.get('institution_name')
                    current_db_status = video_data['download_status']

                    log_details_initial = {
                        "youtube_video_id": yt_video_id,
                        "video_title": video_title,
                        "current_db_download_status": current_db_status,
                        "institution_name": institution_name
                    }

                    if current_db_status == 'completed':
                        logging.info(f"[Main] Video {yt_video_id} (DB ID: {video_db_id}) already marked 'completed'. Skipping submission.")
                        videos_processed_count += 1 
                        continue # To next video in iterator if available
                    # Removed the elif block that skipped 'downloading' status
                    # Now, videos in 'downloading' state will proceed to the download attempt.
                    # You might want to add a log message here if you still want to know it was in 'downloading' state, for example:
                    # elif current_db_status == 'downloading':
                    #     logging.warning(f"[Main] Video {yt_video_id} (DB ID: {video_db_id}) found in 'downloading' state. Attempting download anyway.")
                    
                    logging.info(f"[Main] Submitting download for video {yt_video_id} (DB ID: {video_db_id}, Title: '{video_title if video_title else 'N/A'}')")
                    update_video_download_details_db(conn, video_db_id, 'downloading')
                    add_processing_log_db(conn, video_db_id, 'download', 'submission_to_pool', f"Submitted video {yt_video_id} to download worker pool", log_details_initial)
                    
                    future = executor.submit(download_video, 
                                             video_db_id, yt_video_id, video_url, 
                                             video_title, institution_name, effective_download_dir)
                    active_futures.add(future)
                    # --- End pre-submission processing ---
                
                if not active_futures:
                    # No more videos to submit (video_data is None) and no active tasks left
                    break # Break from the main while True loop

                # Phase 2: Wait for at least one task to complete and process it
                done, active_futures_after_wait = concurrent.futures.wait(active_futures, return_when=concurrent.futures.FIRST_COMPLETED)
                active_futures = active_futures_after_wait # Update active_futures with those still running

                for future in done:
                    try:
                        download_result = future.result()
                        video_db_id_res = download_result["video_db_id"]
                        yt_video_id_res = download_result["yt_video_id"]
                        video_title_res = download_result["video_title"]
                        
                        log_details_completion = {
                            "youtube_video_id": yt_video_id_res,
                            "video_title": video_title_res,
                            "final_status": 'completed' if download_result["success"] else 'failed'
                        }

                        if download_result["success"]:
                            update_video_download_details_db(conn, video_db_id_res, 'completed', download_path_str=download_result["file_path"])
                            add_processing_log_db(conn, video_db_id_res, 'download', 'completed', f"Successfully downloaded: {download_result['file_path']}", log_details_completion)
                            videos_downloaded_this_run += 1
                            logging.info(f"[Main] Download SUCCESS for video {yt_video_id_res} (DB ID: {video_db_id_res}). Path: {download_result['file_path']}")
                        else:
                            update_video_download_details_db(conn, video_db_id_res, 'failed', error_msg_str=download_result["error_message"])
                            add_processing_log_db(conn, video_db_id_res, 'download', 'failed', f"Download error: {download_result['error_message']}", log_details_completion)
                            videos_failed_this_run += 1
                            logging.error(f"[Main] Download FAILED for video {yt_video_id_res} (DB ID: {video_db_id_res}). Error: {download_result['error_message']}")
                    
                    except Exception as exc:
                        # This section is for errors in future.result() or the processing logic itself,
                        # not for download errors caught by download_video function.
                        logging.error(f"[Main] An unexpected error occurred processing a download result: {exc}. Video task may not be identifiable here if future.result() failed early.")
                        videos_failed_this_run += 1 
                    finally:
                        videos_processed_count += 1
                
                # Report progress after processing completed futures
                if videos_processed_count > 0:
                    current_elapsed_time = time.time() - total_start_time
                    logging.info(f"[Main] Progress: {videos_processed_count}/{total_videos_to_attempt} tasks initiated/completed. Successful: {videos_downloaded_this_run}, Failed: {videos_failed_this_run}. Elapsed: {format_time_delta(current_elapsed_time)}.")
            
            logging.info("[Main] All video processing attempts concluded.")

    except KeyboardInterrupt:
        logging.warning("--- Script interrupted by user (Ctrl+C). Workers may take a moment to clean up. ---")
    except Exception as e:
        logging.critical(f"--- An unexpected critical error occurred in the main processing loop: {e} ---", exc_info=True)
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    total_script_runtime = time.time() - total_start_time
    logging.info(f"--- Bulk Video Download Script (Parallel Rolling) Finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    logging.info(f"Total script runtime: {format_time_delta(total_script_runtime)}")
    logging.info(f"Summary: Videos Eligible for Download: {total_videos_to_attempt}, Successfully Downloaded: {videos_downloaded_this_run}, Failed Downloads: {videos_failed_this_run}")
    logging.info(f"Total items processed (including skips/already done before submission): {videos_processed_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download YouTube videos in parallel (rolling submission) from a database queue based on their status.")
    parser.add_argument(
        "--download-dir",
        default=DOWNLOAD_DIR,
        help=f"Directory to download videos into. Overrides DOWNLOAD_DIR env var. Default if env not set: (will raise error if None)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of videos to process in this run."
    )
    parser.add_argument(
        "--status",
        type=str,
        default="pending,failed",
        help="Comma-separated list of video download statuses to process (e.g., 'pending', 'failed', 'pending,failed'). Default: 'pending,failed'"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4, # Default to 4 workers as per user request
        help="Maximum number of parallel download workers. Default: 4"
    )
    args = parser.parse_args()

    if not args.download_dir:
        logging.error("Download directory is not set. Please set DOWNLOAD_DIR environment variable or use --download-dir argument.")
        exit(1)
    
    os.makedirs(args.download_dir, exist_ok=True)

    statuses_to_process_list = [s.strip().lower() for s in args.status.split(',') if s.strip()]
    if not statuses_to_process_list:
        logging.error("No valid statuses provided for processing. Please check the --status argument.")
        exit(1)
    
    if args.workers <= 0:
        logging.error("Number of workers must be a positive integer.")
        exit(1)

    main(args.download_dir, args.limit, statuses_to_process_list, args.workers) 