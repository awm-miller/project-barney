#!/usr/bin/env python3

import os
import sys
import json
import csv
import logging
import argparse
import sqlite3
import concurrent.futures
import threading
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import time
import subprocess

# Import functions from other modules
import transcribe_videos
import download_videos

# --- Configuration ---
LOG_FILE = "transcription_stack.log"
DEFAULT_DATABASE_PATH = r"E:\hate-preachers\test\downloads\transcription_data.db"
DEFAULT_VIDEO_DIR = r"E:\hate-preachers\test\downloads"  # Directory for downloaded videos
DEFAULT_MAX_WORKERS = 4  # Default number of concurrent transcription workers
DEFAULT_AUDIO_FORMAT = "flac"  # Default audio format
DEFAULT_DOWNLOAD_RESULTS = "download_results.csv"  # CSV with video information
DEFAULT_EXPORT_PATH = r"E:\hate-preachers\test\downloads\database_export.json"  # Default path for database export

# --- Thread local storage for database connections ---
thread_local = threading.local()

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Database Functions ---
def get_db_connection(db_path: str) -> sqlite3.Connection:
    """Get a thread-local database connection."""
    if not hasattr(thread_local, "connections"):
        thread_local.connections = {}
        
    thread_id = threading.get_ident()
    if thread_id not in thread_local.connections or thread_local.connections[thread_id] is None:
        # Create a new connection for this thread
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        thread_local.connections[thread_id] = connection
        logging.debug(f"Created new database connection for thread {thread_id}")
    
    return thread_local.connections[thread_id]

def close_thread_connections():
    """Close all database connections for the current thread."""
    if hasattr(thread_local, "connections"):
        thread_id = threading.get_ident()
        if thread_id in thread_local.connections and thread_local.connections[thread_id] is not None:
            try:
                thread_local.connections[thread_id].close()
                thread_local.connections[thread_id] = None
                logging.debug(f"Closed database connection for thread {thread_id}")
            except Exception as e:
                logging.warning(f"Error closing thread-local connection: {e}")

def init_database(db_path: str) -> sqlite3.Connection:
    """Initialize the database with required tables."""
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # Create table for video processing tracking
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS video_processing (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        video_title TEXT,
        channel_id TEXT,
        channel_title TEXT,
        video_url TEXT,
        download_url TEXT,
        video_path TEXT,
        transcript_path TEXT,
        ai_analysis_path TEXT,
        ai_analysis_content TEXT,
        status TEXT,
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create table for processing logs
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS processing_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        video_id INTEGER,
        stage TEXT,
        status TEXT,
        message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (video_id) REFERENCES video_processing (id)
    )
    ''')
    
    conn.commit()
    return conn

def add_video_to_db(conn: sqlite3.Connection, 
                   video_data: Dict) -> int:
    """Add a video entry to the database and return its ID."""
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO video_processing 
    (video_title, channel_id, channel_title, video_url, download_url, video_path, status, error_message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        video_data.get('VIDEO_TITLE', ''),
        video_data.get('CHANNEL_ID', ''),
        video_data.get('CHANNEL_TITLE', ''),
        video_data.get('VIDEO_URL', ''),
        video_data.get('VIDEO_URL', ''),  # Download URL is the same as video URL in this case
        video_data.get('FILE_PATH', ''),
        'initialized',
        video_data.get('ERROR_MESSAGE', '')
    ))
    
    conn.commit()
    return cursor.lastrowid

def video_exists_in_db(conn: sqlite3.Connection, video_url: str) -> bool:
    """Check if a video with the given URL already exists in the database."""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT COUNT(*) FROM video_processing WHERE video_url = ?
    ''', (video_url,))
    count = cursor.fetchone()[0]
    return count > 0

def update_video_status(conn: sqlite3.Connection, 
                       video_id: int, 
                       status: str,
                       video_path: Optional[str] = None,
                       transcript_path: Optional[str] = None,
                       ai_analysis_path: Optional[str] = None,
                       ai_analysis_content: Optional[str] = None,
                       error_message: Optional[str] = None) -> None:
    """Update the status and paths for a video entry."""
    cursor = conn.cursor()
    
    update_fields = ["status = ?", "updated_at = CURRENT_TIMESTAMP"]
    params = [status]
    
    if video_path is not None:
        update_fields.append("video_path = ?")
        params.append(video_path)
    
    if transcript_path is not None:
        update_fields.append("transcript_path = ?")
        params.append(transcript_path)
    
    if ai_analysis_path is not None:
        update_fields.append("ai_analysis_path = ?")
        params.append(ai_analysis_path)
        
    if ai_analysis_content is not None:
        update_fields.append("ai_analysis_content = ?")
        params.append(ai_analysis_content)
    
    if error_message is not None:
        update_fields.append("error_message = ?")
        params.append(error_message)
    
    query = f"UPDATE video_processing SET {', '.join(update_fields)} WHERE id = ?"
    params.append(video_id)
    
    cursor.execute(query, params)
    conn.commit()

def add_log_entry(conn: sqlite3.Connection,
                 video_id: int,
                 stage: str,
                 status: str,
                 message: str) -> None:
    """Add a log entry to the processing_logs table."""
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO processing_logs 
    (video_id, stage, status, message)
    VALUES (?, ?, ?, ?)
    ''', (video_id, stage, status, message))
    
    conn.commit()

def get_videos_by_status(conn: sqlite3.Connection, status: str) -> List[Dict]:
    """Get videos with a specific status from the database."""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT id, video_title, channel_id, channel_title, video_url, download_url, video_path 
    FROM video_processing 
    WHERE status = ?
    ''', (status,))
    
    results = []
    for row in cursor.fetchall():
        results.append({
            'id': row[0],
            'video_title': row[1],
            'channel_id': row[2],
            'channel_title': row[3],
            'video_url': row[4],
            'download_url': row[5],
            'video_path': row[6]
        })
    
    return results

def get_all_videos(conn: sqlite3.Connection) -> List[Dict]:
    """Get all videos from the database."""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT id, video_title, channel_id, channel_title, video_url, download_url, video_path, 
           transcript_path, ai_analysis_path, status, error_message
    FROM video_processing
    ''')
    
    results = []
    column_names = [col[0] for col in cursor.description]
    
    for row in cursor.fetchall():
        results.append(dict(zip(column_names, row)))
    
    return results

# --- Video Processing Functions ---
def load_videos_from_csv(csv_path: str) -> List[Dict]:
    """Load video information from a CSV file."""
    videos = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                videos.append(row)
        logging.info(f"Loaded {len(videos)} videos from {csv_path}")
    except Exception as e:
        logging.error(f"Error loading videos from {csv_path}: {e}")
    
    return videos

def download_video(video: Dict, db_path: str, video_dir: str) -> bool:
    """Download a video from YouTube using a thread-local database connection."""
    # Create a thread-local database connection
    conn = get_db_connection(db_path)
    
    video_id = video['id']
    video_url = video['video_url']
    video_title = video['video_title']
    channel_title = video['channel_title']
    
    # Check if video file already exists
    if os.path.exists(video['video_path']):
        logging.info(f"Video already exists: {video['video_path']}")
        update_video_status(conn, video_id, "downloaded", video_path=video['video_path'])
        add_log_entry(
            conn,
            video_id,
            stage="download",
            status="success",
            message=f"Video already exists: {video['video_path']}"
        )
        return True
    
    # Ensure the download directory exists
    os.makedirs(video_dir, exist_ok=True)
    
    # Extract video ID from URL
    youtube_id = video_url.split('v=')[-1].split('&')[0]
    
    # Generate safe filename
    safe_title = transcribe_videos.sanitize_filename(f"{video_title}-{youtube_id}")
    file_path = os.path.join(video_dir, f"{safe_title}.mp4")
    
    try:
        # Update status to downloading
        update_video_status(conn, video_id, "downloading")
        add_log_entry(
            conn,
            video_id,
            stage="download",
            status="started",
            message=f"Downloading {video_url}"
        )
        
        # Use yt_dlp to download the video
        url = video_url
        ydl_opts = {
            'outtmpl': file_path,
            'quiet': False,  # Show output
            'noprogress': False,  # Show progress bar
            'format': 'best[height<=720]'  # Limit to 720p to speed up downloads
        }
        
        with download_videos.yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        
        # Update status to downloaded
        update_video_status(conn, video_id, "downloaded", video_path=file_path)
        add_log_entry(
            conn,
            video_id,
            stage="download",
            status="success",
            message=f"Downloaded to {file_path}"
        )
        
        return True
    except Exception as e:
        error_msg = str(e)
        logging.error(f"Error downloading video {video_url}: {error_msg}")
        update_video_status(
            conn, 
            video_id, 
            "download_failed", 
            error_message=error_msg
        )
        add_log_entry(
            conn,
            video_id,
            stage="download",
            status="error",
            message=f"Download failed: {error_msg}"
        )
        return False
    finally:
        # Close the thread-local connection
        conn.close()
        if hasattr(thread_local, "connections"):
            delattr(thread_local, "connections")

def download_videos_from_db(conn: sqlite3.Connection, video_dir: str, db_path: str, max_workers: int = DEFAULT_MAX_WORKERS) -> None:
    """Download videos that are in 'initialized' status using thread-local connections."""
    pending_videos = get_videos_by_status(conn, "initialized")
    logging.info(f"Found {len(pending_videos)} videos pending download")
    
    if not pending_videos:
        logging.info("No videos to download")
        return
    
    # Use ThreadPoolExecutor for concurrent downloads
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit download jobs with db_path instead of conn
        future_to_video = {
            executor.submit(download_video, video, db_path, video_dir): video
            for video in pending_videos
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_video):
            video = future_to_video[future]
            try:
                success = future.result()
                if success:
                    logging.info(f"Successfully downloaded: {video['video_title']}")
                else:
                    logging.error(f"Failed to download: {video['video_title']}")
            except Exception as e:
                logging.error(f"Exception during download of {video['video_title']}: {e}")
    
    logging.info("Completed video download phase")

def process_single_audio(video_id: int, video_path: str, db_path: str, temp_audio_dir: str) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """Process a single video's audio extraction using thread-local DB connection."""
    # Create a thread-local database connection
    conn = get_db_connection(db_path)
    
    try:
        if not os.path.exists(video_path):
            logging.error(f"Video file not found: {video_path}")
            update_video_status(conn, video_id, "transcription_failed", error_message="Video file not found")
            add_log_entry(
                conn,
                video_id,
                stage="transcription",
                status="error",
                message=f"Video file not found: {video_path}"
            )
            return None, None, None
        
        # Check if transcript already exists
        video_dir = os.path.dirname(video_path)
        transcript_dir = os.path.join(video_dir, "transcripts")
        os.makedirs(transcript_dir, exist_ok=True)
        
        video_filename = os.path.basename(video_path)
        base_name = os.path.splitext(video_filename)[0]
        transcript_path = os.path.join(transcript_dir, f"{base_name}.txt")
        
        if os.path.exists(transcript_path):
            logging.info(f"Transcript already exists: {transcript_path}")
            update_video_status(conn, video_id, "transcription_complete", transcript_path=transcript_path)
            add_log_entry(
                conn,
                video_id,
                stage="transcription",
                status="success",
                message=f"Transcript already exists: {transcript_path}"
            )
            return None, None, None
        
        # Generate audio filename
        audio_filename = f"{base_name}.{DEFAULT_AUDIO_FORMAT}"
        audio_path = os.path.join(temp_audio_dir, audio_filename)
        
        # Extract audio
        if extract_high_quality_audio(video_path, audio_path):
            update_video_status(conn, video_id, "audio_extracted")
            add_log_entry(
                conn,
                video_id,
                stage="audio_extraction",
                status="success",
                message=f"Audio extracted: {audio_path}"
            )
            return video_id, video_path, audio_path
        else:
            logging.error(f"Failed to extract audio from {video_filename}")
            update_video_status(conn, video_id, "transcription_failed", error_message="Audio extraction failed")
            add_log_entry(
                conn,
                video_id,
                stage="audio_extraction",
                status="error",
                message=f"Failed to extract audio from {video_filename}"
            )
            return None, None, None
    finally:
        # Close the thread-local connection
        conn.close()
        if hasattr(thread_local, "connections"):
            delattr(thread_local, "connections")

def upload_audio_file(video_id: int, video_path: str, audio_path: str, db_path: str, gcs_bucket: str, job_folder: str) -> Tuple[Optional[int], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Upload a single audio file using thread-local DB connection."""
    # Create a thread-local database connection
    conn = get_db_connection(db_path)
    
    try:
        video_filename = os.path.basename(video_path)
        
        try:
            gcs_uri, gcs_blob_name, success = transcribe_videos.upload_audio_to_gcs(
                audio_path, 
                gcs_bucket, 
                job_folder
            )
            
            if success:
                update_video_status(conn, video_id, "audio_uploaded")
                add_log_entry(
                    conn,
                    video_id,
                    stage="gcs_upload",
                    status="success",
                    message=f"Audio uploaded to GCS: {gcs_uri}"
                )
                return video_id, video_path, audio_path, gcs_uri, gcs_blob_name
            else:
                logging.error(f"Failed to upload audio for {video_filename} to GCS")
                update_video_status(conn, video_id, "transcription_failed", error_message="GCS upload failed")
                add_log_entry(
                    conn,
                    video_id,
                    stage="gcs_upload",
                    status="error",
                    message=f"Failed to upload audio to GCS"
                )
                return None, None, None, None, None
        except Exception as e:
            logging.error(f"Exception while uploading audio for {video_filename}: {e}")
            update_video_status(conn, video_id, "transcription_failed", error_message=f"Upload exception: {str(e)}")
            add_log_entry(
                conn,
                video_id,
                stage="gcs_upload",
                status="error",
                message=f"Upload exception: {str(e)}"
            )
            return None, None, None, None, None
    finally:
        # Close the thread-local connection
        conn.close()
        if hasattr(thread_local, "connections"):
            delattr(thread_local, "connections")

def submit_transcription(video_id: int, video_path: str, audio_path: str, gcs_uri: str, gcs_blob_name: str, db_path: str, speech_client) -> Tuple[Optional[int], Optional[str], Optional[str], Optional[object], Optional[str], Optional[str]]:
    """Submit a transcription request using thread-local DB connection."""
    # Create a thread-local database connection
    conn = get_db_connection(db_path)
    
    try:
        try:
            # Create transcript directory in the same folder as the video
            video_dir = os.path.dirname(video_path)
            transcript_dir = os.path.join(video_dir, "transcripts")
            os.makedirs(transcript_dir, exist_ok=True)
            
            # Prepare transcript path
            video_filename = os.path.basename(video_path)
            base_name = os.path.splitext(video_filename)[0]
            transcript_path = os.path.join(transcript_dir, f"{base_name}.txt")
            
            # Configure transcription with highest quality settings and multilingual support
            config = transcribe_videos.speech.RecognitionConfig(
                encoding=transcribe_videos.speech.RecognitionConfig.AudioEncoding.FLAC,
                sample_rate_hertz=44100,        # Match the 44.1kHz of our audio extraction
                # Support multiple languages - primary language is English but enable auto-detection
                language_code="en-GB",          # Primary language
                alternative_language_codes=[    # Secondary languages that might appear
                    "ar-SA",  # Arabic
                    "ur-PK",  # Urdu
                    "fa-IR",  # Farsi/Persian
                    "hi-IN",  # Hindi
                    "bn-IN",  # Bengali
                    "pa-IN",  # Punjabi
                    "tr-TR",  # Turkish
                    "fr-FR",  # French
                    "de-DE",  # German
                    "en-US",
                ],
                enable_word_time_offsets=False,  # Disable word timestamps to focus on fragments
                enable_automatic_punctuation=True,
                enable_spoken_punctuation=True,  # Enables detection of spoken punctuation
                enable_spoken_emojis=True,       # Enables detection of spoken emojis
                model="latest_long",             # Use latest long model which is best for multilingual
                use_enhanced=True,               # Use enhanced model for higher accuracy
                audio_channel_count=1,
                profanity_filter=False,          # Disable profanity filter to get everything
                adaptation={                      # Enable model adaptation for better accuracy
                    "phrase_sets": [],
                    "custom_classes": []
                },
                max_alternatives=1               # Only need the best result
            )
            
            # Create recognition audio from GCS URI
            audio = transcribe_videos.speech.RecognitionAudio(uri=gcs_uri)
            
            # Submit the request
            logging.info(f"Submitting transcription request for {video_filename}")
            operation = speech_client.long_running_recognize(config=config, audio=audio)
            
            update_video_status(conn, video_id, "transcription_submitted")
            add_log_entry(
                conn,
                video_id,
                stage="transcription",
                status="submitted",
                message=f"Transcription request submitted"
            )
            
            return video_id, video_path, audio_path, operation, transcript_path, gcs_blob_name
            
        except Exception as e:
            video_filename = os.path.basename(video_path)
            logging.error(f"Failed to submit transcription for {video_filename}: {e}")
            update_video_status(conn, video_id, "transcription_failed", error_message=f"Transcription submission error: {str(e)}")
            add_log_entry(
                conn,
                video_id,
                stage="transcription",
                status="error",
                message=f"Transcription submission error: {str(e)}"
            )
            return None, None, None, None, None, None
    finally:
        # Close the thread-local connection
        conn.close()
        if hasattr(thread_local, "connections"):
            delattr(thread_local, "connections")

def process_transcription_result(video_id: int, video_path: str, audio_path: str, operation, transcript_path: str, gcs_blob_name: str, db_path: str) -> Tuple[bool, Optional[Tuple]]:
    """Process a transcription result using thread-local DB connection."""
    # Create a thread-local database connection
    conn = get_db_connection(db_path)
    
    try:
        if operation.done():
            try:
                # Get the result
                response = operation.result()
                video_filename = os.path.basename(video_path)
                logging.info(f"Transcription completed for {video_filename}")
                
                # Process and save the transcript with timestamps
                if process_transcript_with_timestamps(response, transcript_path):
                    # Record success
                    update_video_status(
                        conn, 
                        video_id, 
                        "transcription_complete", 
                        transcript_path=transcript_path
                    )
                    
                    add_log_entry(
                        conn,
                        video_id,
                        stage="transcription",
                        status="success",
                        message=f"Transcription completed: {transcript_path}"
                    )
                    
                    return True, (video_id, video_path, audio_path, transcript_path, gcs_blob_name)
                else:
                    # Record processing failure
                    update_video_status(conn, video_id, "transcription_failed", error_message="Failed to process transcription response")
                    add_log_entry(
                        conn,
                        video_id,
                        stage="transcription",
                        status="error",
                        message=f"Failed to process transcription response"
                    )
                    return True, None
                    
            except Exception as e:
                video_filename = os.path.basename(video_path)
                logging.error(f"Error processing transcription for {video_filename}: {e}")
                update_video_status(conn, video_id, "transcription_failed", error_message=f"Transcription processing error: {str(e)}")
                add_log_entry(
                    conn,
                    video_id,
                    stage="transcription",
                    status="error",
                    message=f"Transcription processing error: {str(e)}"
                )
                return True, None
        else:
            # Operation still in progress
            return False, None
    finally:
        # Close the thread-local connection
        conn.close()
        if hasattr(thread_local, "connections"):
            delattr(thread_local, "connections")

def batch_transcribe_videos(conn: sqlite3.Connection, gcs_bucket: str, db_path: str, max_workers: int = DEFAULT_MAX_WORKERS) -> None:
    """
    Process videos in batch mode with thread-local database connections:
    1. Extract audio from all videos
    2. Upload all audio files to GCS
    3. Submit all transcription requests
    4. Process results as they complete
    """
    pending_videos = get_videos_by_status(conn, "downloaded")
    logging.info(f"Found {len(pending_videos)} videos pending transcription")
    
    if not pending_videos:
        logging.info("No videos to transcribe")
        return
    
    # Create a unique job ID
    job_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_folder = f"transcription_job_{job_id}"
    logging.info(f"Created job folder in GCS: {job_folder}")
    
    # Prepare temporary directory for audio files
    temp_audio_dir = os.path.join(DEFAULT_VIDEO_DIR, "temp_audio")
    os.makedirs(temp_audio_dir, exist_ok=True)
    
    # Step 1: Extract audio from all videos (with thread-local connections)
    logging.info(f"=== PHASE 1: Extracting audio from {len(pending_videos)} videos ===")
    audio_extractions = []  # List of (video_id, video_path, audio_path) tuples
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Process audio extractions concurrently
        future_to_video = {
            executor.submit(process_single_audio, video['id'], video['video_path'], db_path, temp_audio_dir): video
            for video in pending_videos
        }
        
        for future in concurrent.futures.as_completed(future_to_video):
            result = future.result()
            if result[0] is not None:  # If extraction succeeded
                audio_extractions.append(result)
    
    logging.info(f"Successfully extracted audio from {len(audio_extractions)} of {len(pending_videos)} videos")
    
    if not audio_extractions:
        logging.error("No audio files were successfully extracted. Exiting.")
        return
    
    # Step 2: Upload all audio files to GCS in parallel (with thread-local connections)
    logging.info(f"=== PHASE 2: Uploading {len(audio_extractions)} audio files to GCS ===")
    gcs_uploads = []  # List of (video_id, video_path, audio_path, gcs_uri, gcs_blob_name) tuples
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Upload audio files concurrently
        future_to_audio = {
            executor.submit(upload_audio_file, vid_id, vid_path, aud_path, db_path, gcs_bucket, job_folder): (vid_id, vid_path, aud_path)
            for vid_id, vid_path, aud_path in audio_extractions
        }
        
        for future in concurrent.futures.as_completed(future_to_audio):
            result = future.result()
            if result[0] is not None:  # If upload succeeded
                gcs_uploads.append(result)
    
    logging.info(f"Successfully uploaded {len(gcs_uploads)} of {len(audio_extractions)} audio files to GCS")
    
    if not gcs_uploads:
        logging.error("No audio files were successfully uploaded to GCS. Exiting.")
        # Clean up extracted audio files
        for _, _, audio_path in audio_extractions:
            try:
                if os.path.exists(audio_path):
                    os.remove(audio_path)
            except Exception as e:
                logging.warning(f"Failed to delete temporary audio file {audio_path}: {str(e)}")
        return
    
    # Step 3: Submit all transcription requests (with thread-local connections)
    logging.info(f"=== PHASE 3: Submitting {len(gcs_uploads)} transcription requests ===")
    
    transcription_ops = []  # List of (video_id, video_path, audio_path, operation, transcript_path, gcs_blob_name) tuples
    
    speech_client = transcribe_videos.speech.SpeechClient()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit transcription requests concurrently
        future_to_gcs = {
            executor.submit(submit_transcription, vid_id, vid_path, aud_path, gcs_uri, gcs_blob, db_path, speech_client): 
            (vid_id, vid_path, aud_path, gcs_uri, gcs_blob)
            for vid_id, vid_path, aud_path, gcs_uri, gcs_blob in gcs_uploads
        }
        
        for future in concurrent.futures.as_completed(future_to_gcs):
            result = future.result()
            if result[0] is not None:  # If submission succeeded
                transcription_ops.append(result)
    
    logging.info(f"Submitted {len(transcription_ops)} transcription operations")
    
    # Step 4: Poll operations and process results (with thread-local connections)
    logging.info(f"=== PHASE 4: Waiting for {len(transcription_ops)} transcription operations to complete ===")
    
    transcription_in_progress = transcription_ops.copy()
    completed_ops = []
    
    # Poll until all operations complete
    poll_interval = 300  # seconds between polling attempts (can be adjusted)
    
    while transcription_in_progress:
        logging.info(f"Checking {len(transcription_in_progress)} operations in progress...")
        still_in_progress = []
        
        for op_data in transcription_in_progress:
            video_id, video_path, audio_path, operation, transcript_path, gcs_blob_name = op_data
            
            # Process the transcription result with thread-local connection
            is_done, result = process_transcription_result(
                video_id, video_path, audio_path, operation, transcript_path, gcs_blob_name, db_path
            )
            
            if is_done:
                if result is not None:
                    completed_ops.append(result)
            else:
                # Operation still in progress
                still_in_progress.append(op_data)
        
        # Update the in-progress list
        transcription_in_progress = still_in_progress
        
        # If there are still operations pending, wait before checking again
        if transcription_in_progress:
            logging.info(f"Waiting for {len(transcription_in_progress)} operations to complete...")
            logging.info(f"Progress: {len(completed_ops)}/{len(transcription_ops)} files transcribed")
            time.sleep(poll_interval)
    
    # Step 5: Clean up GCS files (with thread-local connections if needed)
    logging.info(f"=== PHASE 5: Cleaning up GCS files and temporary audio files ===")
    
    # Clean up GCS files
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Start the delete operations
        future_to_filename = {
            executor.submit(transcribe_videos.delete_gcs_file, gcs_bucket, gcs_blob_name): gcs_blob_name
            for _, _, _, _, gcs_blob_name in completed_ops
        }
        
        # Process results (but don't take action if clean-up fails)
        for future in concurrent.futures.as_completed(future_to_filename):
            gcs_blob_name = future_to_filename[future]
            try:
                success = future.result()
                if not success:
                    logging.warning(f"Could not delete temporary GCS file: {gcs_blob_name}")
            except Exception as e:
                logging.warning(f"Exception while deleting GCS file {gcs_blob_name}: {e}")
    
    # Clean up local audio files
    for _, _, audio_path, _, _ in completed_ops:
        try:
            if os.path.exists(audio_path):
                os.remove(audio_path)
        except Exception as e:
            logging.warning(f"Failed to remove temporary audio file {audio_path}: {str(e)}")
    
    # Final Summary
    logging.info(f"=== SUMMARY ===")
    logging.info(f"Total videos processed: {len(pending_videos)}")
    logging.info(f"Successfully transcribed: {len(completed_ops)}")
    logging.info(f"Failed transcriptions: {len(pending_videos) - len(completed_ops)}")
    logging.info(f"Job ID: {job_id}")
    logging.info(f"GCS Job Folder: {job_folder}")

def process_transcript_with_timestamps(response, transcript_path: str) -> bool:
    """Process transcription response with fragment-level timestamps and language detection."""
    try:
        with open(transcript_path, "w", encoding="utf-8") as f:
            # Iterate through speech segments (results) provided by the API
            for result in response.results:
                # Ensure there are alternatives and grab the best one
                if result.alternatives:
                    best_alternative = result.alternatives[0]
                    
                    # Get timestamps for this fragment
                    if hasattr(result, 'result_end_time'):
                        end_time = result.result_end_time.total_seconds()
                    else:
                        # Fallback to calculating from words if available
                        end_time = max([w.end_time.total_seconds() for w in best_alternative.words]) if best_alternative.words else 0
                    
                    # Start time - either get from first word or use 0
                    start_time = min([w.start_time.total_seconds() for w in best_alternative.words]) if best_alternative.words else 0
                    
                    # Check if we have language detection information
                    language_info = ""
                    if hasattr(result, 'language_code') and result.language_code:
                        language_info = f" [{result.language_code}]"
                    
                    # Write the transcript text with fragment timestamps and language info
                    f.write(f"[{start_time:.2f}s - {end_time:.2f}s]{language_info} {best_alternative.transcript.strip()}\n\n")
        
        logging.info(f"Successfully processed and saved multilingual transcript: {transcript_path}")
        return True
    except Exception as e:
        logging.error(f"Error processing transcription response for {transcript_path}: {e}")
        return False

def export_database_to_json(conn: sqlite3.Connection, output_file: str) -> bool:
    """Export database contents to a JSON file that LLMs can read."""
    try:
        videos = get_all_videos(conn)
        
        # Create a structured format that explains relationships
        data = {
            "description": "Video Transcription Project Database Export",
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "schema": {
                "video_processing": {
                    "description": "This table tracks all videos and their processing status",
                    "fields": {
                        "id": "Unique identifier for each video",
                        "video_title": "Title of the video",
                        "channel_id": "YouTube channel ID",
                        "channel_title": "Name of the YouTube channel",
                        "video_url": "URL to the YouTube video",
                        "download_url": "URL used to download the video",
                        "video_path": "Local path to the downloaded video file",
                        "transcript_path": "Path to the generated transcript file",
                        "ai_analysis_path": "Path to the AI analysis file (if any)",
                        "status": "Current processing status of the video",
                        "error_message": "Error message if any step failed",
                        "created_at": "When the video was added to the database",
                        "updated_at": "When the video was last updated"
                    }
                }
            },
            "videos": videos
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        logging.info(f"Exported database to {output_file}")
        return True
    except Exception as e:
        logging.error(f"Error exporting database to {output_file}: {e}")
        return False

def import_videos_from_csv(conn: sqlite3.Connection, csv_path: str) -> None:
    """Import videos from download_results.csv into the database."""
    videos = load_videos_from_csv(csv_path)
    
    for video in videos:
        # Skip if no video URL
        if not video.get('VIDEO_URL'):
            continue
        
        # Skip if already in database
        if video_exists_in_db(conn, video['VIDEO_URL']):
            logging.info(f"Video already in database: {video.get('VIDEO_TITLE', video['VIDEO_URL'])}")
            continue
        
        # Add to database
        video_id = add_video_to_db(conn, video)
        
        add_log_entry(
            conn,
            video_id,
            stage="import",
            status="success",
            message=f"Imported video from CSV: {video.get('VIDEO_TITLE', video['VIDEO_URL'])}"
        )
    
    logging.info(f"Imported {len(videos)} videos from {csv_path}")

def extract_high_quality_audio(video_path: str, audio_path: str) -> bool:
    """
    Extracts high-quality audio from video file using ffmpeg.
    Uses higher sample rate and bit depth for better transcription quality.
    Returns True on success, False on failure.
    """
    logging.info(f"Extracting high-quality audio from '{os.path.basename(video_path)}' to '{os.path.basename(audio_path)}'")
    command = [
        'ffmpeg',
        '-i', video_path,      # Input file
        '-vn',                 # Disable video recording
        '-acodec', 'flac',     # FLAC codec for lossless audio
        '-ar', '44100',        # Higher sample rate (44.1kHz) for better quality
        '-ac', '1',            # Mono channel
        '-sample_fmt', 's16',  # 16-bit sample format
        '-y',                  # Overwrite output file if it exists
        audio_path
    ]
    try:
        process = subprocess.run(command, check=True, capture_output=True, text=True)
        logging.info(f"Successfully extracted high-quality audio for: {os.path.basename(video_path)}")
        return True
    except FileNotFoundError:
        logging.error("ffmpeg command not found. Please ensure ffmpeg is installed and in your system PATH.")
        return False
    except subprocess.CalledProcessError as e:
        logging.error(f"ffmpeg failed for {os.path.basename(video_path)} with exit code {e.returncode}")
        logging.error(f"ffmpeg stderr: {e.stderr}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during audio extraction for {os.path.basename(video_path)}: {e}")
        return False

def main():
    """Main function to run the transcription stack."""
    parser = argparse.ArgumentParser(description="Video Transcription Stack")
    parser.add_argument("--video-dir", default=DEFAULT_VIDEO_DIR, help="Directory for downloaded videos")
    parser.add_argument("--db-path", default=DEFAULT_DATABASE_PATH, help="Path to SQLite database file")
    parser.add_argument("--gcs-bucket", default=transcribe_videos.DEFAULT_GCS_BUCKET, help="Google Cloud Storage bucket name")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="Maximum number of concurrent workers")
    parser.add_argument("--download-csv", default=DEFAULT_DOWNLOAD_RESULTS, help="CSV file with video download information")
    parser.add_argument("--export-path", default=DEFAULT_EXPORT_PATH, help="Path to export database as JSON")
    parser.add_argument("--skip-download", action="store_true", help="Skip downloading videos and use existing files")
    args = parser.parse_args()
    
    logging.info("Starting Transcription Stack")
    
    # Initialize database
    conn = init_database(args.db_path)
    
    try:
        # 1. Import videos from CSV
        logging.info(f"Importing videos from {args.download_csv}")
        import_videos_from_csv(conn, args.download_csv)
        
        # 2. Download videos if not skipping
        if not args.skip_download:
            logging.info("Downloading videos")
            download_videos_from_db(conn, args.video_dir, args.db_path, args.max_workers)
        else:
            logging.info("Skipping video download")
        
        # 3. Transcribe videos in batch mode
        logging.info("Transcribing videos")
        batch_transcribe_videos(conn, args.gcs_bucket, args.db_path, args.max_workers)
        
        # 4. Export database to JSON
        logging.info(f"Exporting database to {args.export_path}")
        export_database_to_json(conn, args.export_path)
    finally:
        # Close the main thread's database connection
        if 'conn' in locals():
            conn.close()
            logging.debug("Closed main thread's database connection")
        
        # Cleanup thread-local connections
        try:
            close_thread_connections()
            logging.debug("Cleaned up thread-local connections")
        except Exception as e:
            logging.warning(f"Error during thread connection cleanup: {e}")
    
    logging.info("Transcription Stack completed")

if __name__ == "__main__":
    main() 