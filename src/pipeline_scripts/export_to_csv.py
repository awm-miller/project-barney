#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script to export video metadata and links to associated files (subtitles, transcripts)
into a CSV file, and upload these files and the CSV to Google Drive.
"""

import os
import logging
import argparse
import csv
from datetime import datetime
import pathlib
import time
import re # Added for SRT to TXT conversion
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys # Added for robust stream handling in logging

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# Assuming database_manager.py is in the same directory or PYTHONPATH
try:
    from database_manager import DATABASE_NAME as DEFAULT_DB_NAME
    from database_manager import create_connection
except ImportError:
    print("Error: database_manager.py not found. Please ensure it's in the same directory or PYTHONPATH.")
    DEFAULT_DB_NAME = "pipeline_database.db"
    def create_connection(db_file=None): # type: ignore
        print(f"CRITICAL: Using dummy create_connection for {db_file}. Database operations will fail.")
        return None

# --- Configuration ---
DEFAULT_DRIVE_PARENT_FOLDER_ID = "1_9H9e3iSCAOexjd7bdVa-HVedEvWDwlx"
DEFAULT_CSV_FILENAME_PREFIX = "video_export_"
DEFAULT_LOG_FILE = "export_to_csv.log" # Adjusted log file name if script is export_to_csv.py
API_SCOPES = ['https://www.googleapis.com/auth/drive']
DEFAULT_WORKERS = 5

# Base directories for local files - can be overridden by env vars or args
DEFAULT_SUBTITLES_BASE_DIR = os.getenv("SUBTITLES_DIR", "subtitles")
# DEFAULT_ASR_TRANSCRIPTS_BASE_DIR = os.getenv("ASR_TRANSCRIPTS_DIR", "transcripts/asr_word_level") # REMOVED
# DEFAULT_PLAIN_TEXT_SUBTITLES_BASE_DIR = os.getenv("PLAIN_TEXT_SUBTITLES_DIR", "subtitles/plain_text") # REMOVED

# --- Custom Logging Handler for Robust Console Output ---
class SafeStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record) # Unicode string
            stream = self.stream
            # Handle encoding for streams that are not UTF-8 (e.g., Windows console cp1252)
            if hasattr(stream, 'encoding') and stream.encoding and \
               not stream.encoding.lower().startswith('utf'):
                # Encode to the stream's encoding, replacing errors, then decode back.
                safe_msg = msg.encode(stream.encoding, errors='backslashreplace').decode(stream.encoding)
                stream.write(safe_msg + self.terminator)
            else:
                stream.write(msg + self.terminator)
            self.flush()
        except RecursionError: # pragma: no cover
            raise
        except Exception: # pragma: no cover
            self.handleError(record)

# --- Logging Setup ---
def setup_logging(log_file_path=DEFAULT_LOG_FILE):
    """Configures logging to both console and a file with UTF-8 support."""
    log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")

    # File Handler - always use UTF-8
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setFormatter(log_formatter)

    # Stream Handler (for console) - use SafeStreamHandler
    stream_handler = SafeStreamHandler() # Defaults to sys.stderr
    stream_handler.setFormatter(log_formatter)

    # Configure root logger
    # Clear any existing handlers to avoid duplicate logging if script is re-run in same session
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
        
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)

    # Reduce verbosity of Google API client libraries
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
    logging.getLogger('oauth2client').setLevel(logging.WARNING)


# --- Google Drive Utilities ---
def get_drive_service(credentials_path=None):
    """Authenticates and returns a Google Drive service object."""
    try:
        if credentials_path:
            creds = service_account.Credentials.from_service_account_file(
                credentials_path, scopes=API_SCOPES)
        else: # Try to use GOOGLE_APPLICATION_CREDENTIALS environment variable
            creds = service_account.Credentials.from_service_account_file(
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'], scopes=API_SCOPES)
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        logging.info("Successfully authenticated with Google Drive API.")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate with Google Drive API: {e}")
        return None

def check_drive_folder_permissions(drive_service, folder_id):
    """Checks if the service account can add children to the given Drive folder."""
    if not drive_service:
        logging.warning("Drive service not available, skipping permission check.")
        return False
    try:
        folder_metadata = drive_service.files().get(
            fileId=folder_id,
            fields='capabilities'
        ).execute()
        if folder_metadata.get('capabilities', {}).get('canAddChildren'):
            logging.info(f"Service account HAS permission to add files/folders to Drive folder ID: {folder_id}")
            return True
        else:
            logging.warning(f"Service account does NOT have permission to add files/folders to Drive folder ID: {folder_id}")
            return False
    except HttpError as e:
        logging.error(f"HttpError checking permissions for Drive folder ID {folder_id}: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error checking permissions for Drive folder ID {folder_id}: {e}")
        return False


def create_drive_folder_for_csv(drive_service, parent_folder_id, run_timestamp_str):
    """Creates a new, timestamped subfolder in Google Drive."""
    if not drive_service:
        logging.error("Drive service not available, cannot create folder.")
        return None
    
    folder_name = f"CSV_Export_Run_{run_timestamp_str}"
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_folder_id]
    }
    try:
        folder = drive_service.files().create(body=file_metadata, fields='id,webViewLink').execute()
        logging.info(f"Created Google Drive folder: '{folder_name}' (ID: {folder.get('id')}, Link: {folder.get('webViewLink')})")
        return folder.get('id')
    except HttpError as e:
        logging.error(f"Failed to create Google Drive folder '{folder_name}': {e}")
        if e.resp.status == 403:
            logging.error("Permission denied. Check if the service account has 'Writer' or 'Content manager' access to the parent folder.")
        elif e.resp.status == 404:
            logging.error(f"Parent folder ID '{parent_folder_id}' not found.")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while creating folder '{folder_name}': {e}")
        return None


def upload_file_to_drive_for_csv(drive_service, local_file_path, drive_folder_id, file_title=None):
    """Uploads a local file to the specified Google Drive folder and returns its web view link."""
    if not drive_service:
        logging.error(f"Drive service not available, cannot upload file: {local_file_path}")
        return None
    if not os.path.exists(local_file_path):
        logging.warning(f"Local file not found, skipping upload: {local_file_path}")
        return "File not found locally"

    file_name = file_title if file_title else os.path.basename(local_file_path)
    file_metadata = {
        'name': file_name,
        'parents': [drive_folder_id]
    }
    media = MediaFileUpload(local_file_path, resumable=True)
    
    try:
        # Ensure the file_name for metadata is safely encoded if it had to be for logging
        # However, Google Drive API should handle Unicode names directly.
        # The issue was logging, not the upload name itself.
        file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,webViewLink'
        ).execute()
        # Logging of this message is now handled by SafeStreamHandler
        logging.info(f"Successfully uploaded '{file_name}' to Drive. Link: {file.get('webViewLink')}")
        return file.get('webViewLink')
    except HttpError as e:
        logging.error(f"Failed to upload '{file_name}' to Drive: {e}")
        return "Upload Failed"
    except Exception as e:
        logging.error(f"An unexpected error occurred while uploading '{file_name}': {e}")
        return "Upload Failed (Unexpected)"


# --- Database Interaction ---
def get_videos_for_csv_export(conn):
    """
    Retrieves video records from the database that are ready for CSV export.
    Specifically, videos with analysis_status = 'completed'.
    """
    if not conn:
        logging.error("Database connection not available. Cannot fetch videos.")
        return []
    
    cursor = conn.cursor()
    query = """
    SELECT
        v.video_id,
        v.title,
        v.video_url,
        v.published_at,
        v.ai_analysis_content,
        v.text_source,
        v.subtitle_file_path,
        v.plain_text_subtitle_path,
        v.segmented_10w_transcript_path,
        v.description
    FROM videos v
    WHERE v.analysis_status = 'completed'
    ORDER BY v.published_at DESC;
    """
    try:
        cursor.execute(query)
        videos = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        video_list = [dict(zip(column_names, row)) for row in videos]
        logging.info(f"Fetched {len(video_list)} videos with 'completed' analysis status from the database.")
        return video_list
    except Exception as e:
        logging.error(f"Failed to fetch videos for CSV export: {e}")
        return []

# --- Transcript/File Processing ---
def read_file_content(file_path):
    """Reads the content of a local file using UTF-8 encoding."""
    if not file_path or not os.path.exists(file_path):
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None

def format_youtube_url(video_id_or_url):
    return f"https://www.youtube.com/watch?v={video_id_or_url}"


# --- SRT to TXT Conversion Utility ---
def convert_srt_to_plain_text_content(srt_content):
    """
    Converts SRT content (string) to plain text, removing timestamps and sequence numbers.
    Returns a string with the plain text.
    """
    text_lines = []
    # Regex to match timestamp lines (e.g., 00:00:20,000 --> 00:00:24,400)
    # sequence number lines (e.g., 1)
    # and lines with only srt formatting tags like <i> or <b>
    # MODIFIED: Keep timestamp lines, remove only sequence numbers and lines with only tags.
    srt_metadata_pattern = re.compile(r"^\d+$") # Matches only sequence numbers initially
    # For lines that are just tags like <i>, we can check them separately or refine regex
    # Let's refine to also exclude lines that are *only* tags, e.g. "<i>", "<b>"
    # but not lines like "<i>Hello</i> world"
    # A simpler approach for lines with only tags is handled if they become empty after stripping all tags.
    # The original approach: srt_metadata_pattern = re.compile(r"^\d+$|^<[^>]+>$") is actually better for line skipping.

    # Corrected Regex: Match sequence numbers OR lines consisting ONLY of standalone tags.
    # Timestamp lines will NOT match this and will be kept.
    srt_metadata_pattern = re.compile(r"^\d+$|^<[^>]+>$")
    
    for line in srt_content.splitlines():
        stripped_line = line.strip()
        if not srt_metadata_pattern.match(stripped_line) and stripped_line:
            # Further clean common SRT tags like <i>, <b>, etc. from the content itself
            # This will apply to both dialogue lines and timestamp lines if they somehow have tags
            cleaned_line = re.sub(r"<[^>]+>", "", stripped_line)
            if cleaned_line: # Add line only if it's not empty after cleaning
                text_lines.append(cleaned_line)
    return "\n".join(text_lines)

def convert_srt_file_to_plain_text_file(srt_file_path, output_txt_file_path):
    """
    Reads an SRT file, converts its content to plain text, and writes to an output TXT file.
    Returns True on success, False on failure.
    """
    try:
        with open(srt_file_path, 'r', encoding='utf-8') as srt_file:
            srt_content = srt_file.read()
        
        plain_text_content = convert_srt_to_plain_text_content(srt_content)
        
        with open(output_txt_file_path, 'w', encoding='utf-8') as txt_file:
            txt_file.write(plain_text_content)
        logging.info(f"Successfully converted '{srt_file_path}' to '{output_txt_file_path}'")
        return True
    except Exception as e:
        logging.error(f"Failed to convert SRT '{srt_file_path}' to TXT '{output_txt_file_path}': {e}", exc_info=True)
        return False

# --- Upload Worker ---
def upload_files_for_video_worker(video_data, drive_service, drive_folder_id,
                                  subtitles_base_dir):
    """
    Worker function to find [video_id].en-fixed.srt, convert it to .txt, 
    upload the .txt file, and return metadata with the Drive link.
    """
    video_id = video_data.get('video_id', 'UNKNOWN_ID')
    logging.info(f"[Worker VID: {video_id}] Processing video for English fixed SRT to TXT upload.")

    processed_video_data = video_data.copy()
    processed_video_data['drive_link_en_txt'] = "Not applicable" # New key for the TXT link

    try:
        target_srt_filename = f"{video_id}.en-fixed.srt"
        potential_fixed_srt_full_path = os.path.normpath(os.path.join(subtitles_base_dir, target_srt_filename))
        
        logging.info(f"[Worker VID: {video_id}] Explicitly checking for English fixed SRT: {potential_fixed_srt_full_path}")

        path_to_upload_txt = None
        status_message_txt = "Not applicable"

        if os.path.exists(potential_fixed_srt_full_path):
            logging.info(f"[Worker VID: {video_id}] Found English fixed SRT locally: {potential_fixed_srt_full_path}.")
            
            # Define path for the output .txt file (same directory, .txt extension)
            output_txt_filename = f"{video_id}.en-fixed.txt"
            # Place the .txt file in the same directory as the source .srt file
            srt_dir = os.path.dirname(potential_fixed_srt_full_path)
            output_txt_full_path = os.path.normpath(os.path.join(srt_dir, output_txt_filename))

            logging.info(f"[Worker VID: {video_id}] Attempting to convert '{potential_fixed_srt_full_path}' to '{output_txt_full_path}'")
            conversion_successful = convert_srt_file_to_plain_text_file(potential_fixed_srt_full_path, output_txt_full_path)

            if conversion_successful and os.path.exists(output_txt_full_path):
                logging.info(f"[Worker VID: {video_id}] Successfully converted SRT to TXT: {output_txt_full_path}. Will use this for upload.")
                path_to_upload_txt = output_txt_full_path
            else:
                logging.error(f"[Worker VID: {video_id}] Failed to convert SRT to TXT, or TXT file not found post-conversion: {output_txt_full_path}")
                status_message_txt = "SRT to TXT conversion failed"
        else:
            logging.warning(f"[Worker VID: {video_id}] English fixed SRT file not found at: {potential_fixed_srt_full_path}")
            status_message_txt = f"File not found: {target_srt_filename} in subtitles dir"

        # Perform upload if a TXT path was determined
        if path_to_upload_txt:
            logging.info(f"[Worker VID: {video_id}] Attempting upload for English TXT: {path_to_upload_txt}")
            link = upload_file_to_drive_for_csv(drive_service, path_to_upload_txt, drive_folder_id)
            status_message_txt = link if link else "Upload Failed"
        
        processed_video_data['drive_link_en_txt'] = status_message_txt

        # Format URL and Date for CSV
        processed_video_data['video_url'] = format_youtube_url(video_data.get('video_id', video_data.get('video_url')))
        
        raw_date = video_data.get('published_at')
        if raw_date:
            try:
                dt_obj = datetime.fromisoformat(str(raw_date)) if isinstance(raw_date, str) else datetime.fromtimestamp(raw_date) # type: ignore
                processed_video_data['published_at_formatted'] = dt_obj.strftime('%Y-%m-%d')
            except ValueError: # pragma: no cover
                logging.warning(f"[Worker VID: {video_id}] Could not parse date '{raw_date}'. Leaving as is.")
                processed_video_data['published_at_formatted'] = str(raw_date)
        else:
            processed_video_data['published_at_formatted'] = "N/A"
        
        logging.info(f"[Worker VID: {video_id}] Processing finished. TXT Link Status: {processed_video_data['drive_link_en_txt']}")

    except Exception as e:
        logging.error(f"[Worker VID: {video_id}] CRITICAL ERROR during processing: {e}", exc_info=True)
        processed_video_data['drive_link_en_txt'] = processed_video_data.get('drive_link_en_txt', "Worker Error")
        processed_video_data['video_url'] = format_youtube_url(video_data.get('video_id', video_data.get('video_url')))
        raw_date = video_data.get('published_at')
        if raw_date:
            try:
                dt_obj = datetime.fromisoformat(str(raw_date)) if isinstance(raw_date, str) else datetime.fromtimestamp(raw_date) # type: ignore
                processed_video_data['published_at_formatted'] = dt_obj.strftime('%Y-%m-%d')
            except ValueError: # pragma: no cover
                processed_video_data['published_at_formatted'] = str(raw_date)
        else:
            processed_video_data['published_at_formatted'] = "N/A"

    return processed_video_data


# --- CSV Generation ---
def export_data_to_csv(videos_from_db, output_csv_path, drive_service, drive_folder_id,
                         upload_enabled,
                         subtitles_base_dir, 
                         num_workers): 
    """
    Processes video data, handles English TXT (from SRT) uploads (if enabled), and writes to CSV.
    """
    csv_headers = [
        "Title", "URL", "AI summary",
        "Link to English plain text transcript (from .en-fixed.srt)", # UPDATED HEADER
        "Date (Published)"
    ]
    
    processed_videos_for_csv = []

    if upload_enabled:
        if not drive_service or not drive_folder_id: # pragma: no cover
            logging.error("Drive service or folder ID not available. Cannot proceed with uploads for CSV rows.")
            for video_data in videos_from_db:
                processed_data = video_data.copy()
                processed_data['drive_link_en_txt'] = "Upload Skipped (Drive Error)"
                processed_data['video_url'] = format_youtube_url(video_data.get('video_id', video_data.get('video_url')))
                raw_date = video_data.get('published_at')
                processed_data['published_at_formatted'] = datetime.fromisoformat(str(raw_date)).strftime('%Y-%m-%d') if raw_date else "N/A"
                processed_videos_for_csv.append(processed_data)
        else:
            logging.info(f"Starting sequential processing for {len(videos_from_db)} videos (SRT to TXT, then TXT upload)...")
            for video_data in videos_from_db:
                video_id = video_data.get('video_id', 'UNKNOWN_ID_IN_EXPORT_LOOP')
                logging.info(f"Processing video ID: {video_id} sequentially for TXT from SRT.")
                try:
                    processed_data = upload_files_for_video_worker(
                        video_data,
                        drive_service,
                        drive_folder_id,
                        subtitles_base_dir
                    )
                    processed_videos_for_csv.append(processed_data)
                except Exception as exc: # pragma: no cover
                    logging.error(f"Video ID {video_id} generated an exception during sequential processing/upload: {exc}", exc_info=True)
                    failed_data = video_data.copy()
                    failed_data['drive_link_en_txt'] = "Critical Processing Error"
                    failed_data['video_url'] = format_youtube_url(failed_data.get('video_id', failed_data.get('video_url')))
                    raw_date = failed_data.get('published_at')
                    failed_data['published_at_formatted'] = datetime.fromisoformat(str(raw_date)).strftime('%Y-%m-%d') if raw_date else "N/A"
                    processed_videos_for_csv.append(failed_data)
            logging.info("Sequential processing and uploads completed.")
    else: # Uploads disabled
        logging.info("Uploads are disabled. CSV will not contain Drive links for TXT files.")
        for video_data in videos_from_db: # pragma: no cover
            processed_data = video_data.copy()
            processed_data['drive_link_en_txt'] = "Uploads Disabled"
            processed_data['video_url'] = format_youtube_url(video_data.get('video_id', video_data.get('video_url')))
            raw_date = video_data.get('published_at')
            processed_data['published_at_formatted'] = datetime.fromisoformat(str(raw_date)).strftime('%Y-%m-%d') if raw_date else "N/A"
            processed_videos_for_csv.append(processed_data)

    try:
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_headers, extrasaction='ignore')
            writer.writeheader()
            for video_row in processed_videos_for_csv:
                row_to_write = {
                    "Title": video_row.get('title', "N/A"),
                    "URL": video_row.get('video_url', "N/A"),
                    "AI summary": video_row.get('ai_analysis_content', "N/A"),
                    "Link to English plain text transcript (from .en-fixed.srt)": video_row.get('drive_link_en_txt', "N/A"), # UPDATED KEY
                    "Date (Published)": video_row.get('published_at_formatted', "N/A")
                }
                writer.writerow(row_to_write)
        logging.info(f"Successfully exported data to CSV: {output_csv_path}")
    except IOError as e: # pragma: no cover
        logging.error(f"IOError writing CSV file {output_csv_path}: {e}")
    except Exception as e: # pragma: no cover
        logging.error(f"Unexpected error writing CSV file {output_csv_path}: {e}")
        
    return processed_videos_for_csv


# --- Main Execution ---
def main(): # pragma: no cover
    """Main function to orchestrate the export process."""
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Export video data to CSV and Google Drive.")
    parser.add_argument("--output_csv", type=str,
                        help="Filename for the output CSV. Timestamp will be prepended.")
    parser.add_argument("--no_upload", action="store_true",
                        help="Disable all Google Drive uploads (files and final CSV).")
    parser.add_argument("--drive_parent_folder_id", type=str,
                        default=os.getenv('DRIVE_PARENT_FOLDER_ID', DEFAULT_DRIVE_PARENT_FOLDER_ID),
                        help="Google Drive Parent Folder ID for the run-specific subfolder.")
    parser.add_argument("--workers", type=int, default=int(os.getenv('UPLOAD_WORKERS', DEFAULT_WORKERS)),
                        help="Number of parallel workers for file uploads.")
    parser.add_argument("--google_credentials", type=str, default=os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
                        help="Path to Google Cloud service account JSON credentials file.")
    parser.add_argument("--subtitles_dir", type=str, default=DEFAULT_SUBTITLES_BASE_DIR,
                        help="Base directory for local SRT subtitle files.")
    # parser.add_argument("--plain_text_subtitles_dir", type=str, default=DEFAULT_PLAIN_TEXT_SUBTITLES_BASE_DIR, # REMOVED
    #                     help="Base directory for local plain text (e.g., Arabic) subtitle files.") # REMOVED
    # parser.add_argument("--asr_transcripts_dir", type=str, default=DEFAULT_ASR_TRANSCRIPTS_BASE_DIR, # REMOVED
    #                     help="Base directory for local ASR segmented transcript files.") # REMOVED
    parser.add_argument("--db_name", type=str, default=os.getenv('DATABASE_NAME', DEFAULT_DB_NAME),
                        help="Name of the SQLite database file.")
    
    args = parser.parse_args()

    run_timestamp = datetime.now()
    run_timestamp_str = run_timestamp.strftime("%Y%m%d_%H%M%S")

    # Use script name for dynamic log file name
    script_name_without_ext = pathlib.Path(__file__).stem 
    log_filename = f"{script_name_without_ext}_{run_timestamp_str}.log"
    setup_logging(log_filename) # Setup logging first
    
    logging.info(f"--- Starting CSV Export Run: {run_timestamp_str} ---")
    logging.info(f"Arguments: {args}")

    output_csv_filename = args.output_csv if args.output_csv else f"{DEFAULT_CSV_FILENAME_PREFIX}{run_timestamp_str}.csv"
    output_csv_path = pathlib.Path(output_csv_filename).resolve()

    drive_service = None
    run_drive_folder_id = None
    upload_enabled = not args.no_upload

    if upload_enabled:
        logging.info(f"Google Drive uploads are ENABLED. Target parent folder ID: {args.drive_parent_folder_id}")
        drive_service = get_drive_service(args.google_credentials)
        if drive_service:
            if not check_drive_folder_permissions(drive_service, args.drive_parent_folder_id):
                logging.error(f"Permission check failed for parent Drive folder {args.drive_parent_folder_id}. Disabling uploads.")
                upload_enabled = False 
            else:
                run_drive_folder_id = create_drive_folder_for_csv(drive_service, args.drive_parent_folder_id, run_timestamp_str)
                if not run_drive_folder_id:
                    logging.error("Failed to create run-specific folder in Google Drive. File uploads within CSV rows will be marked as skipped/failed.")
                else:
                    logging.info(f"Run-specific Google Drive folder created: {run_drive_folder_id}")
        else:
            logging.error("Failed to initialize Google Drive service. Uploads will be disabled.")
            upload_enabled = False
    else:
        logging.info("Google Drive uploads are DISABLED by user.")

    db_conn = create_connection(args.db_name)
    if not db_conn:
        logging.critical(f"Failed to connect to database: {args.db_name}. Exiting.")
        return

    videos_to_export = get_videos_for_csv_export(db_conn)
    db_conn.close() 

    if not videos_to_export:
        logging.info("No videos found for export. Exiting.")
        return

    logging.info(f"Processing {len(videos_to_export)} videos for CSV export.")
    
    export_data_to_csv(
        videos_from_db=videos_to_export,
        output_csv_path=str(output_csv_path),
        drive_service=drive_service,
        drive_folder_id=run_drive_folder_id, 
        upload_enabled=upload_enabled,
        subtitles_base_dir=args.subtitles_dir,
        # plain_text_subtitles_base_dir=args.plain_text_subtitles_dir, # REMOVED
        # asr_transcripts_base_dir=args.asr_transcripts_dir, # REMOVED
        num_workers=args.workers
    )

    # if upload_enabled and drive_service and run_drive_folder_id and os.path.exists(output_csv_path):
    #     logging.info(f"Attempting to upload the final CSV '{output_csv_path}' to Drive folder {run_drive_folder_id}...")
    #     csv_drive_link = upload_file_to_drive_for_csv(drive_service, str(output_csv_path), run_drive_folder_id)
    #     if csv_drive_link and "Failed" not in csv_drive_link and "not found" not in csv_drive_link :
    #         logging.info(f"Successfully uploaded CSV to Google Drive: {csv_drive_link}")
    #     else:
    #         logging.error(f"Failed to upload CSV '{output_csv_path}' to Google Drive. Status: {csv_drive_link}")
    # elif upload_enabled:
    #     logging.warning(f"Skipping upload of final CSV '{output_csv_path}'. Conditions not met (Drive service/folder issues or CSV not found).")

    logging.info(f"--- CSV Export Run {run_timestamp_str} Finished ---")
    print(f"Log file created at: {pathlib.Path(log_filename).resolve()}")
    print(f"Output CSV created at: {output_csv_path}")

if __name__ == "__main__": # pragma: no cover
    main() 