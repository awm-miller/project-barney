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
        v.title,
        v.video_url,
        v.segmented_10w_transcript_path, 
        v.published_at,
        v.ai_analysis_content 
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

# --- Main Export Logic ---
def export_data_to_csv(videos_data, csv_filename, drive_service, csv_run_drive_folder_id):
    """Exports video data to a CSV file and uploads segmented transcripts sequentially."""
    
    csv_rows_to_write = []

    if not videos_data:
        logging.info("No video data provided. Writing empty CSV with headers.")
    else:
        for video_row_tuple in videos_data:
            video_db_id, video_title, video_url, segmented_10w_local_path, published_at_ts, ai_summary = video_row_tuple
            
            logging.info(f"Processing video ID {video_db_id}: '{video_title}'")
            
            published_date_str = "N/A"
            if published_at_ts:
                try:
                    if isinstance(published_at_ts, str):
                        date_part = published_at_ts.split('T')[0] if 'T' in published_at_ts else published_at_ts.split(' ')[0]
                        dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
                        published_date_str = dt_obj.strftime('%Y-%m-%d')
                    elif isinstance(published_at_ts, datetime):
                        published_date_str = published_at_ts.strftime('%Y-%m-%d')
                    else:
                        temp_str = str(published_at_ts).split(' ')[0]
                        datetime.strptime(temp_str, '%Y-%m-%d')
                        published_date_str = temp_str
                except ValueError as ve:
                    logging.warning(f"Could not parse published_at timestamp '{published_at_ts}' for video ID {video_db_id}: {ve}. Using raw.")
                    published_date_str = published_at_ts[:10] if isinstance(published_at_ts, str) and len(published_at_ts) >= 10 else "N/A"
                except Exception as e:
                    logging.error(f"Unexpected error parsing date for video {video_db_id}: {published_at_ts} - {e}")
                    published_date_str = "N/A"

            # Upload segmented transcript (if path exists)
            segmented_transcript_drive_link = "N/A"
            if segmented_10w_local_path and os.path.exists(segmented_10w_local_path):
                logging.info(f"Uploading segmented transcript: {segmented_10w_local_path} to Drive folder ID: {csv_run_drive_folder_id}")
                drive_link = upload_file_to_drive_for_csv(drive_service, segmented_10w_local_path, csv_run_drive_folder_id)
                if drive_link:
                    segmented_transcript_drive_link = drive_link
                else:
                    logging.warning(f"Failed to upload or get link for {segmented_10w_local_path}. Setting link to 'Upload Failed'")
                    segmented_transcript_drive_link = "Upload Failed"
            elif segmented_10w_local_path:
                logging.warning(f"Segmented transcript path for video ID {video_db_id} exists in DB ('{segmented_10w_local_path}') but file not found locally.")
                segmented_transcript_drive_link = "Segmented transcript file missing locally"
            else:
                segmented_transcript_drive_link = "No segmented transcript path in DB"

            csv_rows_to_write.append({
                "Title": video_title or f"Video ID {video_db_id}",
                "URL": format_youtube_url(video_url) if video_url else "N/A",
                "AI summary": ai_summary or "Summary not available",
                "Link to Segmented Transcript (10-word)": segmented_transcript_drive_link,
                "Date": published_date_str
            })
        
    fieldnames = ["Title", "URL", "AI summary", "Link to Segmented Transcript (10-word)", "Date"]
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            if csv_rows_to_write: 
                writer.writerows(csv_rows_to_write)
                logging.info(f"Successfully wrote {len(csv_rows_to_write)} data rows to {csv_filename}")
            # If csv_rows_to_write is empty (either no videos_data or processed videos yielded no rows for some reason)
            # it will just write the header, which is fine.
            elif not videos_data:
                 logging.info(f"Successfully wrote CSV with headers, but no data rows (no videos to process): {csv_filename}")
            else: # videos_data was not empty, but csv_rows_to_write is (e.g. all uploads failed AND we decided not to add row)
                 logging.info(f"Successfully wrote CSV with headers, but no data rows were ultimately added: {csv_filename}")

        return len(csv_rows_to_write) # Number of data rows written
    except IOError as e:
        logging.error(f"Could not write to CSV file {csv_filename}: {e}")
        return None 

def main():
    parser = argparse.ArgumentParser(description="Export video data to CSV, with links to segmented transcripts uploaded to Google Drive.")
    parser.add_argument(
        "--output_csv",
        default=f"{DEFAULT_CSV_FILENAME_PREFIX}{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        help="Output CSV file name."
    )
    parser.add_argument(
        "--drive_parent_folder_id",
        default=GOOGLE_DRIVE_PARENT_FOLDER_ID_FOR_CSV,
        help="Google Drive Parent Folder ID for creating the transcript export subfolder."
    )
    parser.add_argument(
        "--drive_export_folder_name",
        help="Specific name for the Google Drive folder to be created for this export run's transcripts. Defaults to CSV filename."
    )

    args = parser.parse_args()
    
    output_csv_file = args.output_csv
    drive_parent_folder_id = args.drive_parent_folder_id
    drive_export_folder_name = args.drive_export_folder_name if args.drive_export_folder_name else os.path.splitext(os.path.basename(output_csv_file))[0] + "_transcripts"


    logging.info(f"Starting CSV export to {output_csv_file}. Segmented transcripts will be uploaded to Google Drive sequentially.")

    if not drive_parent_folder_id or drive_parent_folder_id == "YOUR_DRIVE_PARENT_FOLDER_ID_HERE":
        logging.error("Google Drive parent folder ID for CSV exports is not configured. "
                      "Set GCLOUD_FOLDER or GOOGLE_DRIVE_PARENT_FOLDER_ID_FOR_CSV in .env or script, or use --drive_parent_folder_id.")
        sys.exit(1)

    drive_service = get_google_drive_service()
    if not drive_service:
        logging.error("Failed to initialize Google Drive service. Cannot upload transcripts. Exiting.")
        sys.exit(1)

    # Create a unique folder for this CSV export run's transcripts
    csv_run_drive_folder_id = create_drive_folder_for_csv(drive_service, drive_export_folder_name, drive_parent_folder_id)
    if not csv_run_drive_folder_id:
        logging.error(f"Failed to create Google Drive folder '{drive_export_folder_name}' for transcripts. Exiting.")
        sys.exit(1)
    
    logging.info(f"Segmented transcripts for this CSV export will be uploaded to Drive folder: '{drive_export_folder_name}' (ID: {csv_run_drive_folder_id})")
    
    conn = create_connection(DATABASE_NAME)
    if not conn:
        logging.error("Could not connect to the database. Exiting.")
        return
    
    try:
        videos_to_export = get_videos_for_csv_export(conn)
        
        if not videos_to_export:
            logging.info("No videos found for CSV export. Creating empty CSV with headers.")
            fieldnames = ["Title", "URL", "AI summary", "Link to Segmented Transcript (10-word)", "Date"]
            try:
                with open(output_csv_file, 'w', encoding='utf-8', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                logging.info(f"Created empty CSV with headers: {output_csv_file}")
                print(f"\nCSV Export process completed. No videos found to export.")
                print(f"Output file (empty with headers): {output_csv_file}")
            except IOError as e:
                logging.error(f"Could not create empty CSV file {output_csv_file}: {e}")
                print(f"\nCSV Export failed. Could not create empty CSV. Check log {LOG_FILE}.")
            conn.close()
            return
        
        rows_written = export_data_to_csv(videos_to_export, output_csv_file, drive_service, csv_run_drive_folder_id)
        
        if rows_written is not None: 
            print(f"\nCSV Export process completed successfully!")
            print(f"Output file: {output_csv_file}")
            print(f"Segmented transcripts uploaded to Google Drive folder: '{drive_export_folder_name}' (ID: {csv_run_drive_folder_id})")
            print(f"Total data rows written to CSV: {rows_written}")
            if rows_written == 0 and videos_to_export: # videos_to_export was not empty at the start
                 logging.info("CSV file created with headers, but no data rows were generated from the processed videos.")
        else: # rows_written is None, meaning an IOError occurred
            print(f"\nCSV Export failed during file writing. Check the log file {LOG_FILE} for details.")
        
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")
    
    logging.info("CSV Export process completed.")

if __name__ == "__main__":
    main() 