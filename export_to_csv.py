#!/usr/bin/env python3

import os
import sys
import csv
import sqlite3
import logging
import argparse
from datetime import datetime
from dotenv import load_dotenv
import re # For stripping timestamps
import concurrent.futures # Added for parallel uploads

# --- Google API Imports ---
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.exceptions import RefreshError

# Import from our database manager
from database_manager import create_connection, DATABASE_NAME

# --- Load environment variables ---
load_dotenv()

# --- Configuration ---
LOG_FILE = "export_to_csv.log"
DEFAULT_CSV_FILENAME_PREFIX = "video_export_csv_"
# Try GCLOUD_FOLDER first, then the more specific one, then a placeholder
GOOGLE_DRIVE_PARENT_FOLDER_ID_FOR_CSV = os.getenv("GCLOUD_FOLDER") or \
                                      os.getenv("GOOGLE_DRIVE_PARENT_FOLDER_ID_FOR_CSV") or \
                                      "YOUR_DRIVE_PARENT_FOLDER_ID_HERE"
SCOPES = ['https://www.googleapis.com/auth/drive.file'] # Only drive.file needed

# Define directories for finding subtitle files, consistent with other scripts
DEFAULT_SUBTITLE_DIR = "subtitles" 
DEFAULT_PLAIN_TEXT_SUBTITLE_DIR = "plain_text_subtitles"
DEFAULT_MAX_WORKERS_EXPORT = 1 # Default workers for export uploads

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Google Service Functions (copied and adapted from export_to_gsheet.py) ---
def get_google_drive_service():
    """Authenticates and returns Google Drive API service object."""
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        logging.error("GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
        return None
    try:
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        drive_service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        logging.info("Successfully authenticated and built Google Drive API service.")
        return drive_service
    except RefreshError as e:
        logging.error(f"Error refreshing Google credentials: {e}")
        return None
    except Exception as e:
        logging.error(f"Error initializing Google Drive API service: {e}")
        return None

def create_drive_folder_for_csv(drive_service, folder_name, parent_folder_id):
    """Creates a folder in Google Drive for CSV export transcripts and returns its ID."""
    if not parent_folder_id or parent_folder_id == "YOUR_DRIVE_PARENT_FOLDER_ID_HERE":
        logging.error("Google Drive parent folder ID is not set correctly. Please update it in .env or script (e.g., GCLOUD_FOLDER or GOOGLE_DRIVE_PARENT_FOLDER_ID_FOR_CSV).")
        return None
    try:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_folder_id]
        }
        folder = drive_service.files().create(body=file_metadata, fields='id').execute()
        logging.info(f"Created Google Drive folder for CSV export: '{folder_name}' with ID: {folder.get('id')}")
        return folder.get('id')
    except Exception as e:
        logging.error(f"Error creating Google Drive folder '{folder_name}': {e}")
        return None

def upload_file_to_drive_for_csv(drive_service, local_file_path, folder_id):
    """Uploads a local file to a specific Google Drive folder and returns its webViewLink."""
    if not drive_service or not folder_id: # Added check for disabled uploads
        logging.debug(f"Drive service or folder_id not available. Skipping upload for {local_file_path}")
        return "Uploads disabled or folder error"
    if not os.path.exists(local_file_path):
        logging.warning(f"Local file not found, cannot upload: {local_file_path}")
        return None
    try:
        file_name = os.path.basename(local_file_path)
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        media = MediaFileUpload(local_file_path, mimetype='text/plain')
        uploaded_file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, webViewLink'
        ).execute()
        logging.info(f"Uploaded '{file_name}' to Drive for CSV link. Link: {uploaded_file.get('webViewLink')}")
        return uploaded_file.get('webViewLink')
    except Exception as e:
        logging.error(f"Error uploading file '{local_file_path}' to Drive: {e}")
        return None

# --- Transcript Processing Functions ---
def read_transcript_file(transcript_path):
    """Read the content of a transcript file."""
    if not transcript_path or not os.path.exists(transcript_path):
        logging.warning(f"Transcript file not found or path is null: {transcript_path}")
        return "Transcript file not found or path missing."
    try:
        with open(transcript_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logging.error(f"Error reading transcript file {transcript_path}: {e}")
        return f"Error reading transcript: {e}"

def strip_timestamps_from_transcript_content(content: str) -> str:
    """Removes word-level timestamps like '[0.000 - 2.200] ' from transcript lines."""
    if not content or content.startswith("Transcript file not found") or content.startswith("Error reading transcript"):
        return content
    stripped_lines = []
    for line in content.splitlines():
        line_without_timestamp = re.sub(r"^\s*\[\s*\d+(\.\d+)?\s*-\s*\d+(\.\d+)?\s*\]\s*", "", line)
        # Ensure we're not adding empty strings if a line was ONLY a timestamp (though unlikely)
        if line_without_timestamp.strip(): 
            stripped_lines.append(line_without_timestamp.strip()) # Also strip leading/trailing whitespace from the word itself
    return " ".join(stripped_lines) # Join with spaces instead of newlines

# --- URL Formatting Function ---
def format_youtube_url(video_url: str) -> str:
    """Helper function to format YouTube URLs to a consistent watch?v= format."""
    if not video_url:
        return "N/A"
    if "youtube.com/watch?v=" in video_url:
        return video_url.split('&')[0] # Remove extra params like playlist
    if "youtu.be/" in video_url:
        video_id = video_url.split("youtu.be/")[-1].split('?')[0]
        return f"https://www.youtube.com/watch?v={video_id}"
    # Add other common formats if necessary
    logging.warning(f"Unrecognized YouTube URL format, returning as is: {video_url}")
    return video_url

# --- Database Function ---
def get_videos_for_csv_export(conn):
    """Get videos that have completed AI summarization for CSV export."""
    cursor = conn.cursor()
    query = """
    SELECT 
        v.id,
        v.video_id,
        v.title,
        v.video_url,
        v.published_at,
        v.ai_analysis_content,
        v.text_source,
        v.subtitle_file_path,
        v.plain_text_subtitle_path,
        v.segmented_10w_transcript_path
    FROM videos v
    WHERE 
        v.analysis_status = 'completed'
    ORDER BY v.id ASC 
    """
    try:
        cursor.execute(query)
        videos = cursor.fetchall()
        logging.info(f"Found {len(videos)} videos with completed AI summaries for CSV export.")
        return videos
    except sqlite3.Error as e:
        logging.error(f"Database error fetching videos for CSV export: {e}")
        return []

# --- Worker function for parallel uploads ---
def upload_files_for_video_worker(video_data_tuple, drive_service, target_folder_id):
    """Worker function to handle file uploads for a single video."""
    (video_db_id, youtube_video_id, video_title, video_url, published_at_ts, 
     ai_summary, text_source, db_subtitle_file_path, db_plain_text_subtitle_path, 
     db_segmented_10w_transcript_path) = video_data_tuple

    logging.debug(f"[Worker {youtube_video_id}] Processing uploads.")

    link_eng_srt = "N/A"
    link_arabic_txt = "N/A"
    link_asr_transcript = "N/A"

    # 1. English SRT
    expected_eng_srt_filename = f"{youtube_video_id}.en-fixed.srt"
    eng_srt_local_path = os.path.join(DEFAULT_SUBTITLE_DIR, expected_eng_srt_filename)
    if os.path.exists(eng_srt_local_path):
        drive_link = upload_file_to_drive_for_csv(drive_service, eng_srt_local_path, target_folder_id)
        link_eng_srt = drive_link if drive_link else "Upload Failed"
    else:
        link_eng_srt = "English SRT not found locally"
    
    # 2. Arabic Plain Text from Subtitles
    if text_source == 'SUBTITLE' and db_plain_text_subtitle_path and os.path.exists(db_plain_text_subtitle_path):
        drive_link = upload_file_to_drive_for_csv(drive_service, db_plain_text_subtitle_path, target_folder_id)
        link_arabic_txt = drive_link if drive_link else "Upload Failed"
    elif text_source == 'SUBTITLE' and db_plain_text_subtitle_path:
        link_arabic_txt = "Arabic Plain Text Subtitle file missing locally"
    elif text_source == 'SUBTITLE':
        link_arabic_txt = "No Arabic Plain Text Subtitle path in DB"
    else:
        link_arabic_txt = "N/A (Text source not SUBTITLE)"

    # 3. ASR Transcript (conditional)
    subtitles_linked = (link_eng_srt not in ["N/A", "English SRT not found locally", "Upload Failed", "Uploads disabled or folder error"]) or \
                       (link_arabic_txt not in ["N/A", "Arabic Plain Text Subtitle file missing locally", "Upload Failed", "No Arabic Plain Text Subtitle path in DB", "N/A (Text source not SUBTITLE)", "Uploads disabled or folder error"])

    if not subtitles_linked:
        if db_segmented_10w_transcript_path and os.path.exists(db_segmented_10w_transcript_path):
            drive_link = upload_file_to_drive_for_csv(drive_service, db_segmented_10w_transcript_path, target_folder_id)
            link_asr_transcript = drive_link if drive_link else "Upload Failed (ASR)"
        elif db_segmented_10w_transcript_path:
            link_asr_transcript = "ASR transcript file missing locally"
        else:
            link_asr_transcript = "No ASR transcript path in DB"
    else:
        link_asr_transcript = "N/A (Subtitles linked)"
    
    # Return all data needed for the CSV row
    return {
        "video_db_id": video_db_id, # Keep for potential reference, though not directly in CSV
        "youtube_video_id": youtube_video_id,
        "Title": video_title or f"Video ID {video_db_id}",
        "URL": format_youtube_url(video_url) if video_url else "N/A",
        "published_at_ts": published_at_ts, # Pass through for date formatting in main thread
        "AI summary": ai_summary or "Summary not available",
        "link_eng_srt": link_eng_srt,
        "link_arabic_txt": link_arabic_txt,
        "link_asr_transcript": link_asr_transcript
    }

# --- Main Export Logic ---
def export_data_to_csv(videos_data, csv_filename, drive_service, csv_run_drive_folder_id, max_workers):
    """Exports video data to a CSV file, using workers for parallel file uploads."""
    
    csv_rows_to_write = []
    fieldnames = [
        "Title", "URL", "AI summary", 
        "Link to fixed srt subtitles (ENGLISH)",
        "Link to plaintext subtitles (ARABIC)",
        "Link to segmented transcript (ASR)",
        "Date"
    ]

    if not videos_data:
        logging.info("No video data provided. Writing empty CSV with headers.")
    else:
        processed_video_count = 0
        total_videos = len(videos_data)
        logging.info(f"Starting parallel file uploads for {total_videos} videos using {max_workers} workers...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_video_data = {
                executor.submit(upload_files_for_video_worker, video_row_tuple, drive_service, csv_run_drive_folder_id): video_row_tuple
                for video_row_tuple in videos_data
            }

            for future in concurrent.futures.as_completed(future_to_video_data):
                original_video_data = future_to_video_data[future]
                worker_video_id = original_video_data[1] # youtube_video_id from the tuple
                try:
                    upload_results = future.result() # This is the dict from the worker
                    
                    # Format date here from passed-through timestamp
                    published_date_str = "N/A"
                    published_at_ts_res = upload_results["published_at_ts"]
                    if published_at_ts_res:
                        try:
                            if isinstance(published_at_ts_res, str):
                                date_part = published_at_ts_res.split('T')[0] if 'T' in published_at_ts_res else published_at_ts_res.split(' ')[0]
                                dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
                                published_date_str = dt_obj.strftime('%Y-%m-%d')
                            elif isinstance(published_at_ts_res, datetime):
                                published_date_str = published_at_ts_res.strftime('%Y-%m-%d')
                            else:
                                temp_str = str(published_at_ts_res).split(' ')[0]
                                datetime.strptime(temp_str, '%Y-%m-%d')
                                published_date_str = temp_str
                        except Exception as e_date:
                            logging.warning(f"Error parsing date '{published_at_ts_res}' for video {upload_results['youtube_video_id']} in main thread: {e_date}")
                            published_date_str = "N/A"

                    csv_rows_to_write.append({
                        "Title": upload_results["Title"],
                        "URL": upload_results["URL"],
                        "AI summary": upload_results["AI summary"],
                        "Link to fixed srt subtitles (ENGLISH)": upload_results["link_eng_srt"],
                        "Link to plaintext subtitles (ARABIC)": upload_results["link_arabic_txt"],
                        "Link to segmented transcript (ASR)": upload_results["link_asr_transcript"],
                        "Date": published_date_str
                    })
                    logging.info(f"[Main Export] Successfully processed uploads for video: {upload_results['youtube_video_id']}")
                except Exception as exc:
                    logging.error(f"[Main Export] Error processing result for video {worker_video_id}: {exc}", exc_info=True)
                    # Optionally, add a row indicating failure for this video if needed
                finally:
                    processed_video_count += 1
                    if processed_video_count % 10 == 0 or processed_video_count == total_videos:
                        logging.info(f"[Main Export] File upload progress: {processed_video_count}/{total_videos} videos' files processed.")
        
        logging.info("All parallel file upload tasks completed.")

    # (CSV writing logic as before)
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            if csv_rows_to_write: 
                # Sort rows by original video_db_id if order is important and IDs are available and unique
                # For now, appending as completed. If order needs to match DB, more complex handling is needed.
                writer.writerows(csv_rows_to_write)
                logging.info(f"Successfully wrote {len(csv_rows_to_write)} data rows to {csv_filename}")
            elif not videos_data: # videos_data was empty from the start
                 logging.info(f"Successfully wrote CSV with headers, but no data rows (no videos to process): {csv_filename}")
            else: # videos_data was not empty, but csv_rows_to_write is (e.g., all workers failed before returning useful data)
                 logging.info(f"Successfully wrote CSV with headers, but no data rows were ultimately added from processing: {csv_filename}")

        return len(csv_rows_to_write)
    except IOError as e:
        logging.error(f"Could not write to CSV file {csv_filename}: {e}")
        return None 

def main():
    parser = argparse.ArgumentParser(description="Export video data to CSV, with links to relevant files uploaded to Google Drive.")
    parser.add_argument(
        "--output_csv",
        type=str,
        help=f"Filename for the output CSV. Default: {DEFAULT_CSV_FILENAME_PREFIX}<timestamp>.csv"
    )
    parser.add_argument(
        "--no_upload",
        action="store_true",
        help="Disable uploading files to Google Drive (CSV will contain local paths or N/A)."
    )
    parser.add_argument(
        "--drive_parent_folder_id",
        type=str,
        default=GOOGLE_DRIVE_PARENT_FOLDER_ID_FOR_CSV,
        help="Google Drive Parent Folder ID where the CSV and transcript subfolder will be created."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_MAX_WORKERS_EXPORT,
        help=f"Maximum number of parallel workers for file uploads. Default: {DEFAULT_MAX_WORKERS_EXPORT}"
    )

    args = parser.parse_args()

    logging.info(f"--- Starting CSV Export at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

    conn = create_connection(DATABASE_NAME)
    if not conn:
        logging.error("Failed to connect to the database. Exiting.")
        sys.exit(1)

    videos_data_tuples = get_videos_for_csv_export(conn)
    conn.close()

    if not videos_data_tuples:
        logging.info("No videos found with completed AI analysis for export. Exiting.")
        # Create an empty CSV with headers if requested, or just exit
        if args.output_csv:
             csv_filename_to_use = args.output_csv
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename_to_use = f"{DEFAULT_CSV_FILENAME_PREFIX}{timestamp}.csv"
        
        # Call with empty data to write headers
        export_data_to_csv([], csv_filename_to_use, None, None, args.workers) 
        logging.info(f"Empty CSV with headers written to {csv_filename_to_use}")
        sys.exit(0)

    drive_service = None
    csv_run_drive_folder_id = None # ID of the subfolder for this specific CSV run

    if not args.no_upload:
        drive_service = get_google_drive_service()
        if not drive_service:
            logging.warning("Could not initialize Google Drive service. Files will not be uploaded. Proceeding without uploads.")
        elif args.drive_parent_folder_id == "YOUR_DRIVE_PARENT_FOLDER_ID_HERE":
            logging.error("Google Drive Parent Folder ID is set to placeholder. Please configure it in .env or via --drive_parent_folder_id. Disabling uploads.")
            drive_service = None # Disable drive operations
        else:
            run_timestamp_for_folder = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_run_folder_name = f"CSV_Export_Run_{run_timestamp_for_folder}"
            csv_run_drive_folder_id = create_drive_folder_for_csv(drive_service, csv_run_folder_name, args.drive_parent_folder_id)
            if not csv_run_drive_folder_id:
                logging.error("Failed to create run-specific folder in Google Drive. Disabling uploads for this run.")
                drive_service = None # Disable drive operations
            else:
                logging.info(f"Successfully created Google Drive folder for this run: {csv_run_folder_name} (ID: {csv_run_drive_folder_id})")
    else:
        logging.info("Google Drive upload is disabled via --no_upload flag.")


    # Determine CSV filename
    if args.output_csv:
        csv_filename_to_use = args.output_csv
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename_to_use = f"{DEFAULT_CSV_FILENAME_PREFIX}{timestamp}.csv"

    num_rows_written = export_data_to_csv(videos_data_tuples, csv_filename_to_use, drive_service, csv_run_drive_folder_id, args.workers)

    if num_rows_written is not None:
        logging.info(f"CSV export process completed. {num_rows_written} data rows written to {csv_filename_to_use}")
        if drive_service and csv_run_drive_folder_id: # If uploads were active and folder was created
            logging.info(f"Attempting to upload the main CSV file '{csv_filename_to_use}' to Drive folder ID: {csv_run_drive_folder_id}")
            csv_drive_link = upload_file_to_drive_for_csv(drive_service, csv_filename_to_use, csv_run_drive_folder_id)
            if csv_drive_link:
                logging.info(f"Main CSV file uploaded to Google Drive. Link: {csv_drive_link}")
            else:
                logging.error(f"Failed to upload main CSV file '{csv_filename_to_use}' to Google Drive.")
    else:
        logging.error("CSV export process failed.")

    logging.info(f"--- CSV Export Finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

if __name__ == "__main__":
    main() 