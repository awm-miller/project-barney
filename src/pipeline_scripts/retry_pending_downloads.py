#!/usr/bin/env python3

import os
import logging
import sqlite3
import json
from datetime import datetime
from dotenv import load_dotenv
import yt_dlp

# Import from our database manager
from database_manager import create_connection, DATABASE_NAME

# --- Configuration ---
load_dotenv()

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")
if not DOWNLOAD_DIR:
    raise ValueError("DOWNLOAD_DIR not found in .env file. Please set it to your desired video download path.")

LOG_FILE = "retry_specific_downloads.log"
SCRIPT_NAME = "retry_specific_downloads.py"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_video_details(conn, video_db_ids=None):
    """Get video details from database. Fetches specific IDs if provided, otherwise fetches all pending.
    Args:
        conn: The database connection object.
        video_db_ids: A list of specific video database IDs to fetch. If None or empty, fetches all pending.
    Returns:
        A list of dictionaries, each containing details for a video.
    """
    cursor = conn.cursor()
    
    select_clause = """
    SELECT
        v.id as video_db_id,
        v.video_id as youtube_video_id,
        v.video_url,
        v.title as video_title,
        c.institution_name,
        v.download_status
    FROM videos v
    LEFT JOIN channels c ON v.channel_id = c.channel_id
    """
    
    params = []
    if video_db_ids:
        placeholders = ','.join('?' for _ in video_db_ids)
        sql = f"{select_clause} WHERE v.id IN ({placeholders})"
        params = video_db_ids
        logging.info(f"Fetching details for specific video DB IDs: {video_db_ids}")
    else:
        sql = f"{select_clause} WHERE v.download_status = ?"
        params.append('pending')
        logging.info("Fetching details for all videos with download_status = 'pending'")
    
    try:
        cursor.execute(sql, params)
        videos_data = cursor.fetchall()
        return [dict(zip([column[0] for column in cursor.description], row)) for row in videos_data]
    except sqlite3.Error as e:
        logging.error(f"Database error fetching video details: {e}")
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
        cursor.execute(sql, (video_db_pk, stage_str, status_str, message_str, details_json_str, SCRIPT_NAME))
        conn.commit()
        logging.debug(f"Logged to processing_logs for video_record_id {video_db_pk}: {stage_str} - {status_str}")
    except sqlite3.Error as e:
        logging.error(f"Database error adding processing log for video_record_id {video_db_pk}: {e}")

def sanitize_filename(name: str) -> str:
    """Sanitize filename to be safe for all operating systems."""
    if name is None:
        return ""
    name = name.replace(':', '_').replace('/', '_').replace('\\\\', '_').replace('?', '_').replace('*', '_')
    name = "".join(c if c.isalnum() or c in ' ._-()[]' else '_' for c in name).strip()
    return name[:150]

def download_video(video_db_id: int, yt_video_id: str, video_url: str, video_title: str | None, institution_name: str | None, current_download_dir: str):
    """Downloads a single video using yt-dlp."""
    current_institution_name_safe = sanitize_filename(institution_name if institution_name else "Unknown_Institution")
    video_title_safe = sanitize_filename(video_title if video_title else "Untitled_Video")
    fname_base = f"{current_institution_name_safe}_{video_title_safe}_{yt_video_id}"
    fname = f"{fname_base}.mp4"
    out_path = os.path.join(current_download_dir, fname)
    os.makedirs(current_download_dir, exist_ok=True)
    
    result = {
        "video_db_id": video_db_id,
        "yt_video_id": yt_video_id,
        "success": False,
        "file_path": None,
        "error_message": None,
        "video_title": video_title
    }

    ydl_opts = {
        'outtmpl': out_path,
        'quiet': True,
        'noprogress': True,
        'format': 'bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/best[ext=mp4][height<=720]/best',
        'retries': 2,
        'fragment_retries': 2,
        'socket_timeout': 60,
        'verbose': False,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            result["success"] = True
            result["file_path"] = out_path
        else:
            result["error_message"] = "File not found or empty after download attempt"

    except yt_dlp.utils.DownloadError as e:
        error_message = str(e)[:250]
        if "Video unavailable" in str(e): error_message = "Video unavailable"
        elif "Private video" in str(e): error_message = "Private video"
        elif "HTTP Error 403" in str(e) or "Access denied" in str(e): error_message = "Access denied/Forbidden (403)"
        elif "Premiere will begin" in str(e): error_message = "Video is a premiere, not yet available"
        elif "live event will begin" in str(e): error_message = "Video is a live event, not yet available"
        result["error_message"] = error_message
    except Exception as e:
        result["error_message"] = str(e)[:250]
    
    return result

def main():
    """Main function to retry downloads for specific video IDs."""
    logging.info("--- Starting Video Download Script ---")
    
    # Set to None or [] to download all pending videos
    # Set to a list of integers (DB IDs) to retry specific videos
    video_db_ids = None # Modified to download pending videos
    
    conn = create_connection(DATABASE_NAME)
    if not conn:
        logging.error("Could not connect to the database. Exiting.")
        return

    try:
        # Get video details
        videos_to_process = get_video_details(conn, video_db_ids)
        if not videos_to_process:
            logging.error("No videos found to process. Exiting.")
            return

        if video_db_ids:
            logging.info(f"Found {len(videos_to_process)} specific videos to process.")
        else:
            logging.info(f"Found {len(videos_to_process)} videos with 'pending' download status to process.")
        
        # Process each video
        for video_data in videos_to_process:
            video_db_id = video_data['video_db_id']
            yt_video_id = video_data['youtube_video_id']
            video_url = video_data['video_url']
            video_title = video_data['video_title']
            institution_name = video_data.get('institution_name')
            
            logging.info(f"Processing video {yt_video_id} (DB ID: {video_db_id}, Title: '{video_title if video_title else 'N/A'}')")
            
            # Update status to downloading
            update_video_download_details_db(conn, video_db_id, 'downloading')
            
            # Attempt download
            download_result = download_video(
                video_db_id, yt_video_id, video_url,
                video_title, institution_name, DOWNLOAD_DIR
            )
            
            # Update database with result
            if download_result["success"]:
                update_video_download_details_db(
                    conn, video_db_id, 'completed',
                    download_path_str=download_result["file_path"]
                )
                logging.info(f"Successfully downloaded video {yt_video_id} to {download_result['file_path']}")
            else:
                update_video_download_details_db(
                    conn, video_db_id, 'failed',
                    error_msg_str=download_result["error_message"]
                )
                logging.error(f"Failed to download video {yt_video_id}. Error: {download_result['error_message']}")

    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    logging.info("--- Video Download Script Finished ---")

if __name__ == "__main__":
    main() 