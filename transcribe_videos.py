#!/usr/bin/env python3

import os
import argparse
import json
import logging
import subprocess
import time
import sys
# import uuid # Not strictly needed if job_id is timestamp based
import concurrent.futures
from datetime import datetime
from google.cloud import speech
from google.cloud import storage
from google.api_core.exceptions import NotFound, GoogleAPIError
from dotenv import load_dotenv
import sqlite3
from typing import Optional, Tuple # Added Optional and Tuple

# Import from our database manager
from database_manager import create_connection, DATABASE_NAME

# --- Load environment variables ---
load_dotenv()

# --- Configuration ---
LOG_FILE = "transcribe_videos.log"
SCRIPT_NAME = "transcribe_videos.py"

TRANSCRIPT_DIR = os.getenv("TRANSCRIPTS_DIR")
if not TRANSCRIPT_DIR:
    raise ValueError("TRANSCRIPTS_DIR not found in .env file. Please set it.")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
if not GCS_BUCKET_NAME:
    raise ValueError("GCS_BUCKET_NAME not found in .env file.")

DEFAULT_AUDIO_FORMAT = "flac"
MAX_DIRECT_API_SIZE = 10 * 1024 * 1024  # 10MB (though not used for long-running)
MAX_WORKERS = 4
UPLOAD_TIMEOUT = 1800  # 30 minutes

# --- Logging Setup ---
class UnicodeStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, 'a', 'utf-8'),
        UnicodeStreamHandler(sys.stdout) # Use custom handler for stdout
    ]
)

# --- Database Helper Functions ---

# Set logger name
logger = logging.getLogger(__name__)

def get_videos_for_transcription_from_db(conn, limit=None):
    """Fetches videos ready for transcription (Completed download, Pending or Failed transcription)."""
    cursor = conn.cursor()
    # Select videos that have been successfully downloaded AND
    # whose transcription status is either pending or failed.
    sql = """
    SELECT id, download_path, title, transcription_status
    FROM videos
    WHERE download_status = 'completed' 
      AND (transcription_status = 'pending' OR transcription_status = 'failed')
    ORDER BY added_at ASC
    """
    params = []
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    
    try:
        cursor.execute(sql, params)
        videos = cursor.fetchall()
        # Convert to list of dictionaries for easier access
        video_list = [dict(zip([column[0] for column in cursor.description], row)) for row in videos]
        logging.info(f"Found {len(video_list)} videos for transcription from database (pending or failed).")
        return video_list
    except sqlite3.Error as e:
        logging.error(f"Database error fetching videos for transcription: {e}")
        return []

def update_video_transcription_status_db(conn, video_db_id: int, status_str: str, 
                                       transcript_path_str: Optional[str]=None, gcs_blob_str: Optional[str]=None, 
                                       gcp_op_name_str: Optional[str]=None, error_msg_str: Optional[str]=None,
                                       initiated: bool = False, completed: bool = False):
    """Updates video transcription status and related fields in the database."""
    cursor = conn.cursor()
    
    # Initialize lists for SET clauses and parameters
    set_clauses = ["transcription_status = ?"]
    parameters = [status_str]
    
    # Conditionally add fields to update
    if transcript_path_str is not None:
        set_clauses.append("transcription_path = ?")
        parameters.append(transcript_path_str)
    if gcs_blob_str is not None:
        set_clauses.append("gcs_blob_name = ?")
        parameters.append(gcs_blob_str)
    if gcp_op_name_str is not None:
        set_clauses.append("gcp_operation_name = ?")
        parameters.append(gcp_op_name_str)
    if error_msg_str is not None:
        set_clauses.append("transcription_error_message = ?")
        parameters.append(error_msg_str)
    elif status_str == 'completed' or status_str == 'pending': # Clear error on success or reset to pending
        set_clauses.append("transcription_error_message = NULL")
        
    # Handle timestamps
    if initiated:
        # Set initiated time only if it's not already set for this attempt
        # Or, if retrying a failed status, update initiated time
        set_clauses.append("transcription_initiated_at = CURRENT_TIMESTAMP")
        # When initiating (or re-initiating), clear completion time and potentially error
        set_clauses.append("transcription_completed_at = NULL") 
        if status_str != 'failed': # Don't clear error message if we are explicitly setting failed status
             set_clauses.append("transcription_error_message = NULL")
    elif completed:
        set_clauses.append("transcription_completed_at = CURRENT_TIMESTAMP")
        # Clear error on successful completion
        set_clauses.append("transcription_error_message = NULL")
        
    # Always update last_updated_at
    set_clauses.append("last_updated_at = CURRENT_TIMESTAMP")
    
    sql = f"""UPDATE videos SET {", ".join(set_clauses)} WHERE id = ?"""
    parameters.append(video_db_id)
    
    try:
        cursor.execute(sql, parameters)
        conn.commit()
        logging.info(f"Updated video (DB ID: {video_db_id}) to transcription_status: '{status_str}'.")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Database error updating video (DB ID: {video_db_id}) transcription status: {e}")

def add_processing_log_db(conn, video_db_id: Optional[int], stage_str: str, status_str: str, message_str: str, details_dict: Optional[dict]=None):
    """Adds an entry to the processing_logs table."""
    if video_db_id is None:
        logging.warning(f"Skipped logging to processing_logs (video_db_id is None). Message: {message_str}")
        return
        
    details_json_str = json.dumps(details_dict) if details_dict else None
    sql = """
    INSERT INTO processing_logs (video_record_id, stage, status, message, details, timestamp, source_script)
    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?);
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (video_db_id, stage_str, status_str, message_str, details_json_str, SCRIPT_NAME))
        conn.commit()
        logging.debug(f"Logged to processing_logs for video_record_id {video_db_id}: {stage_str} - {status_str}")
    except sqlite3.Error as e:
        logging.error(f"Database error adding processing log for video_record_id {video_db_id}: {e}")

# --- Helper Functions (Adapted/Original) ---

def ensure_dir_exists(directory_path: str):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logging.info(f"Created directory: {directory_path}")

def sanitize_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in ' ._-' else '_' for c in name).strip()

def extract_audio(video_path: str, audio_path: str) -> bool:
    logging.info(f"Extracting audio from '{os.path.basename(video_path)}' to '{os.path.basename(audio_path)}'")
    command = [
        'ffmpeg', '-i', video_path, '-vn',
        '-acodec', 'flac' if DEFAULT_AUDIO_FORMAT == 'flac' else 'pcm_s16le',
        '-ar', '16000', '-ac', '1', '-y', audio_path
    ]
    try:
        process = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8', errors='replace')
        logging.debug(f"ffmpeg stdout: {process.stdout}")
        logging.debug(f"ffmpeg stderr: {process.stderr}")
        logging.info(f"Successfully extracted audio for: {os.path.basename(video_path)}")
        return True
    except FileNotFoundError:
        logging.error("ffmpeg command not found. Ensure ffmpeg is installed and in system PATH.")
        return False
    except subprocess.CalledProcessError as e:
        logging.error(f"ffmpeg failed for {os.path.basename(video_path)} (exit code {e.returncode}): {e.stderr}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error during audio extraction for {os.path.basename(video_path)}: {e}")
        return False

def upload_audio_to_gcs(audio_path: str, gcs_bucket_name: str, job_folder: str) -> tuple[Optional[str], Optional[str], bool]:
    audio_basename = sanitize_filename(os.path.basename(audio_path))
    gcs_blob_name = f"{job_folder}/{audio_basename}"
    gcs_uri = f"gs://{gcs_bucket_name}/{gcs_blob_name}"
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(gcs_bucket_name) # More direct way to get bucket
        if not bucket.exists():
            logging.info(f"Bucket {gcs_bucket_name} not found, creating it...")
            # You might need to specify location for bucket creation:
            # bucket = storage_client.create_bucket(gcs_bucket_name, location="US") # Example location
            bucket = storage_client.create_bucket(gcs_bucket_name)
            logging.info(f"Bucket {gcs_bucket_name} created.")
            
        blob = bucket.blob(gcs_blob_name)
        
        logging.info(f"Uploading {audio_basename} to GCS: {gcs_uri} (Size: {os.path.getsize(audio_path) / (1024*1024):.2f} MB)")
        upload_start_time = time.time()
        blob.upload_from_filename(audio_path, timeout=UPLOAD_TIMEOUT)
        duration = time.time() - upload_start_time
        speed = (os.path.getsize(audio_path) / (1024*1024)) / duration if duration > 0 else 0
        logging.info(f"Upload of {audio_basename} completed in {duration:.2f}s ({speed:.2f} MB/s)")
        return gcs_uri, gcs_blob_name, True
    except NotFound: # Specifically for bucket not found if create_bucket is removed/fails
        logging.error(f"GCS Bucket {gcs_bucket_name} not found and creation failed or was skipped.")
        return None, gcs_blob_name, False # gcs_blob_name is still useful for logging
    except GoogleAPIError as e: # Catch other Google API errors
        logging.error(f"Google API error uploading {audio_basename} to GCS: {e}")
        return None, gcs_blob_name, False
    except Exception as e:
        logging.error(f"Unexpected error uploading {audio_basename} to GCS: {e}")
        return None, gcs_blob_name, False

def delete_gcs_file(gcs_bucket_name: str, gcs_blob_name: str) -> bool:
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_blob_name)
        if blob.exists():
            blob.delete()
            logging.info(f"Deleted GCS file: gs://{gcs_bucket_name}/{gcs_blob_name}")
        else:
            logging.warning(f"Attempted to delete non-existent GCS file: gs://{gcs_bucket_name}/{gcs_blob_name}")
        return True
    except Exception as e:
        logging.warning(f"Failed to delete GCS file gs://{gcs_bucket_name}/{gcs_blob_name}: {e}")
        return False

def process_transcription_response(response, transcript_out_path: str) -> bool:
    """Processes the Speech API response and writes a formatted transcript with word-level timestamps."""
    try:
        # Log basic response info for diagnostics
        result_count = len(response.results) if hasattr(response, 'results') else 0
        logging.info(f"Processing response with {result_count} results for {transcript_out_path}")
        
        with open(transcript_out_path, "w", encoding="utf-8") as f:
            # Track if we ever get valid word timings
            any_word_timings_found = False
            
            for i, result in enumerate(response.results):
                # Skip empty results
                if not hasattr(result, 'alternatives') or not result.alternatives:
                    logging.warning(f"Result {i} has no alternatives, skipping")
                    continue
                
                # Get best alternative
                alternative = result.alternatives[0]
                
                # Check if this alternative has word timing data
                has_words = hasattr(alternative, 'words') and alternative.words
                
                if not has_words:
                    # No word-level info - fall back to segment-level timestamp
                    segment_text = alternative.transcript.strip()
                    logging.warning(f"No word-level timings in segment #{i+1}, using segment-level only")
                    f.write(f"[0.000] {segment_text}\n\n")
                    continue
                
                # Debug the word count
                word_count = len(alternative.words)
                logging.info(f"Processing segment #{i+1} with {word_count} words")
                
                # Process words with timing data
                for word_index, word_info in enumerate(alternative.words):
                    word_text = word_info.word if hasattr(word_info, 'word') else ""
                    
                    # Safely extract timing data with proper error handling
                    try:
                        # Check if start_time attribute exists and is not None
                        if (hasattr(word_info, 'start_time') and word_info.start_time is not None and 
                            hasattr(word_info.start_time, 'total_seconds')):
                            start_time = word_info.start_time.total_seconds()
                        else:
                            start_time = 0.0
                            
                        # Check if end_time attribute exists and is not None
                        if (hasattr(word_info, 'end_time') and word_info.end_time is not None and 
                            hasattr(word_info.end_time, 'total_seconds')):
                            end_time = word_info.end_time.total_seconds()
                        else:
                            end_time = 0.0
                            
                        # Only consider it a valid timing if both start and end are non-zero
                        if start_time > 0.0 or end_time > 0.0:
                            any_word_timings_found = True
                            
                        # Write the word with its timing
                        f.write(f"[{start_time:.3f} - {end_time:.3f}] {word_text}\n")
                    except Exception as word_error:
                        # Log and fall back to zero timestamps if anything goes wrong
                        logging.error(f"Error extracting timing for word {word_index} in segment {i+1}: {word_error}")
                        f.write(f"[0.000 - 0.000] {word_text}\n")
                
                # Add an extra newline between segments for readability
                f.write("\n")
        
        # Log success with info about whether timings were found
        if any_word_timings_found:
            logging.info(f"Successfully processed and saved transcript with word-level timings: {transcript_out_path}")
        else:
            logging.warning(f"Processed transcript, but no valid word timings were found: {transcript_out_path}")
            
        return True
    except Exception as e:
        logging.error(f"Error processing transcription response for {transcript_out_path}: {e}")
        return False

# --- Main Execution ---
def main(current_transcript_dir: str, current_gcs_bucket_name: str, max_videos_to_process: Optional[int] = None):
    total_start_time = time.time()
    logging.info(f"--- Starting Video Transcription Script (DB Integrated) at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    logging.info(f"Transcripts will be saved to: {current_transcript_dir}")
    logging.info(f"Audio will be temporarily uploaded to GCS bucket: {current_gcs_bucket_name}")
    if max_videos_to_process:
        logging.info(f"Processing a maximum of {max_videos_to_process} videos this run.")

    ensure_dir_exists(current_transcript_dir)
    
    conn = create_connection(DATABASE_NAME)
    if not conn:
        logging.error("Could not connect to the database. Exiting.")
        return

    videos_to_process = get_videos_for_transcription_from_db(conn, limit=max_videos_to_process)
    if not videos_to_process:
        logging.info("No videos found needing transcription. Exiting.")
        if conn: conn.close()
        return

    job_id_for_gcs = datetime.now().strftime("%Y%m%d_%H%M%S")
    gcs_job_folder = f"transcription_job_{job_id_for_gcs}"
    logging.info(f"Using GCS job folder: {gcs_job_folder}")

    audio_files_to_upload = [] # Tuples: (video_db_id, video_path, audio_path)
    failed_extractions = []    # Tuples: (video_db_id, video_path, error)

    # Step 1: Extract audio
    logging.info(f"=== PHASE 1: Extracting audio for {len(videos_to_process)} videos ===")
    for video_data in videos_to_process:
        video_db_id = video_data['id']
        video_dl_path = video_data['download_path']
        video_title = video_data['title']

        if not video_dl_path or not os.path.exists(video_dl_path):
            err_msg = f"Video download_path missing or file not found: {video_dl_path}"
            logging.error(err_msg)
            update_video_transcription_status_db(conn, video_db_id, 'failed', error_msg_str=err_msg)
            add_processing_log_db(conn, video_db_id, 'transcription', 'failed', "Audio extraction skipped", {"error": err_msg, "video_path": video_dl_path})
            failed_extractions.append((video_db_id, video_dl_path, err_msg))
            continue
            
        # Reset/update status before starting
        update_video_transcription_status_db(conn, video_db_id, 'pending_extraction', initiated=True)
        add_processing_log_db(conn, video_db_id, 'transcription', 'initiated_extraction', f"Starting audio extraction for {os.path.basename(video_dl_path)}")

        safe_base_name = sanitize_filename(os.path.splitext(os.path.basename(video_dl_path))[0])
        audio_filename = f"{safe_base_name}.{DEFAULT_AUDIO_FORMAT}"
        # Place temporary audio files in the transcript_dir for organization
        temp_audio_path = os.path.join(current_transcript_dir, audio_filename) 

        if extract_audio(video_dl_path, temp_audio_path):
            audio_files_to_upload.append((video_db_id, video_dl_path, temp_audio_path))
            update_video_transcription_status_db(conn, video_db_id, 'pending_gcs_upload')
            add_processing_log_db(conn, video_db_id, 'transcription', 'completed_extraction', f"Audio extracted to {temp_audio_path}")
        else:
            err_msg = "Audio extraction failed"
            update_video_transcription_status_db(conn, video_db_id, 'failed', error_msg_str=err_msg)
            add_processing_log_db(conn, video_db_id, 'transcription', 'failed_extraction', err_msg, {"video_path": video_dl_path})
            failed_extractions.append((video_db_id, video_dl_path, err_msg))
    
    logging.info(f"Successfully extracted audio for {len(audio_files_to_upload)} videos. Failed for {len(failed_extractions)}.")
    if not audio_files_to_upload:
        logging.warning("No audio files were successfully extracted. Exiting phase.")
        if conn: conn.close()
        return
    
    # Step 2: Upload audio to GCS
    gcs_upload_data = [] # Tuples: (video_db_id, video_dl_path, temp_audio_path, gcs_uri, gcs_blob_name)
    failed_uploads = []  # Tuples: (video_db_id, temp_audio_path, error)
    logging.info(f"=== PHASE 2: Uploading {len(audio_files_to_upload)} audio files to GCS ===")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_upload_info = {
            executor.submit(upload_audio_to_gcs, audio_path, current_gcs_bucket_name, gcs_job_folder): (v_db_id, v_dl_path, audio_path)
            for v_db_id, v_dl_path, audio_path in audio_files_to_upload
        }
        for future in concurrent.futures.as_completed(future_to_upload_info):
            v_db_id, v_dl_path, temp_audio_path = future_to_upload_info[future]
            try:
                gcs_uri_res, gcs_blob_name_res, success = future.result()
                if success and gcs_uri_res:
                    gcs_upload_data.append((v_db_id, v_dl_path, temp_audio_path, gcs_uri_res, gcs_blob_name_res))
                    update_video_transcription_status_db(conn, v_db_id, 'pending_api_submit', gcs_blob_str=gcs_blob_name_res)
                    add_processing_log_db(conn, v_db_id, 'transcription', 'completed_gcs_upload', f"Uploaded to {gcs_uri_res}", {"gcs_blob_name": gcs_blob_name_res})
                else:
                    err_msg = f"GCS upload failed (GCS URI: {gcs_uri_res}, Blob: {gcs_blob_name_res})"
                    update_video_transcription_status_db(conn, v_db_id, 'failed', error_msg_str=err_msg, gcs_blob_str=gcs_blob_name_res) # Store blob even if failed for cleanup
                    add_processing_log_db(conn, v_db_id, 'transcription', 'failed_gcs_upload', err_msg, {"audio_path": temp_audio_path, "gcs_blob_name": gcs_blob_name_res})
                    failed_uploads.append((v_db_id, temp_audio_path, err_msg))
            except Exception as exc:
                err_msg = f"GCS upload generated an exception: {exc}"
                logging.error(f"Exception for {temp_audio_path}: {exc}")
                update_video_transcription_status_db(conn, v_db_id, 'failed', error_msg_str=err_msg)
                add_processing_log_db(conn, v_db_id, 'transcription', 'failed_gcs_upload', err_msg, {"audio_path": temp_audio_path})
                failed_uploads.append((v_db_id, temp_audio_path, str(exc)))
                
    logging.info(f"Successfully uploaded {len(gcs_upload_data)} audio files. Failed for {len(failed_uploads)}.")
    if not gcs_upload_data:
        logging.warning("No audio files successfully uploaded to GCS. Exiting phase.")
        # Consider cleaning up local audio files from audio_files_to_upload here
        if conn: conn.close()
        return
    
    # Step 3: Submit transcription requests
    transcription_operations = [] # Tuples: (video_db_id, gcp_operation, transcript_final_path, gcs_blob_to_delete, local_audio_to_delete)
    failed_submissions = []       # Tuples: (video_db_id, gcs_uri, error)
    logging.info(f"=== PHASE 3: Submitting {len(gcs_upload_data)} transcription requests to Google Speech-to-Text ===")
    speech_client = speech.SpeechClient()
    for v_db_id, v_dl_path, temp_audio_path, gcs_uri, gcs_blob_name in gcs_upload_data:
        # Define final transcript path
        base_name_orig_video = os.path.splitext(os.path.basename(v_dl_path))[0]
        safe_base_name = sanitize_filename(base_name_orig_video)
        transcript_filename = f"{safe_base_name}_transcript.txt"
        transcript_final_path = os.path.join(current_transcript_dir, transcript_filename)
            
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.FLAC if DEFAULT_AUDIO_FORMAT == 'flac' else speech.RecognitionConfig.AudioEncoding.LINEAR16,
            sample_rate_hertz=16000,
            language_code="ar-XA", # Changed from en-US to Arabic
            enable_automatic_punctuation=True,
            enable_word_time_offsets=True # Ensure this is TRUE for word-level timestamps
        )
        audio_for_api = speech.RecognitionAudio(uri=gcs_uri)

        try:
            logging.info(f"Submitting transcription for {gcs_uri} (Video DB ID: {v_db_id})")
            operation = speech_client.long_running_recognize(config=config, audio=audio_for_api)
            transcription_operations.append((v_db_id, operation, transcript_final_path, gcs_blob_name, temp_audio_path))
            update_video_transcription_status_db(conn, v_db_id, 'transcribing_api', gcp_op_name_str=operation.operation.name)
            add_processing_log_db(conn, v_db_id, 'transcription', 'submitted_to_api', f"GCP Operation: {operation.operation.name}", {"gcs_uri": gcs_uri})
        except GoogleAPIError as e:
            err_msg = f"Google API error submitting transcription for {gcs_uri}: {e}"
            logging.error(err_msg)
            update_video_transcription_status_db(conn, v_db_id, 'failed', error_msg_str=err_msg)
            add_processing_log_db(conn, v_db_id, 'transcription', 'failed_api_submission', err_msg, {"gcs_uri": gcs_uri})
            failed_submissions.append((v_db_id, gcs_uri, str(e)))
            # Also schedule GCS blob for cleanup even if submission failed
            delete_gcs_file(current_gcs_bucket_name, gcs_blob_name) # Attempt to clean up GCS
            if os.path.exists(temp_audio_path): os.remove(temp_audio_path) # Clean up local audio
        except Exception as e:
            err_msg = f"Unexpected error submitting transcription for {gcs_uri}: {e}"
            logging.error(err_msg)
            update_video_transcription_status_db(conn, v_db_id, 'failed', error_msg_str=err_msg)
            add_processing_log_db(conn, v_db_id, 'transcription', 'failed_api_submission', err_msg, {"gcs_uri": gcs_uri})
            failed_submissions.append((v_db_id, gcs_uri, str(e)))
            delete_gcs_file(current_gcs_bucket_name, gcs_blob_name)
            if os.path.exists(temp_audio_path): os.remove(temp_audio_path)


    logging.info(f"Successfully submitted {len(transcription_operations)} transcription operations. Failed submissions: {len(failed_submissions)}.")
    if not transcription_operations:
        logging.warning("No transcription operations successfully submitted. Exiting phase.")
        if conn: conn.close()
        return
    
    # Step 4: Poll operations and process results
    logging.info(f"=== PHASE 4: Waiting for {len(transcription_operations)} transcriptions to complete ===")
    completed_count = 0
    failed_processing_count = 0
    
    # Use a copy for iteration as we might remove items or re-queue them
    active_operations = list(transcription_operations) 
    files_for_cleanup_gcs = [] # (gcs_blob_name)
    files_for_cleanup_local = [] # (local_audio_path)

    # Timeout for the entire polling phase (e.g., 2 hours for very long jobs)
    polling_phase_timeout = 7200 # seconds
    polling_phase_start_time = time.time()
    
    while active_operations:
        if time.time() - polling_phase_start_time > polling_phase_timeout:
            logging.error(f"Polling phase timed out after {polling_phase_timeout/3600:.1f} hours. {len(active_operations)} operations still pending.")
            for v_db_id, op, _, gcs_blob, local_audio in active_operations:
                update_video_transcription_status_db(conn, v_db_id, 'failed', error_msg_str="Polling timeout")
                add_processing_log_db(conn, v_db_id, 'transcription', 'failed_polling', f"Operation {op.operation.name} timed out.")
                files_for_cleanup_gcs.append(gcs_blob)
                files_for_cleanup_local.append(local_audio)
            break # Exit while loop

        logging.info(f"Polling {len(active_operations)} active transcription operations...")
        next_active_operations = []
        for v_db_id, operation, transcript_final_path, gcs_blob, local_audio in active_operations:
            if operation.done():
                files_for_cleanup_gcs.append(gcs_blob) # Mark GCS for cleanup once done
                files_for_cleanup_local.append(local_audio) # Mark local audio for cleanup

                try:
                    response = operation.result() # Can raise exceptions
                    logging.info(f"Transcription completed for Video DB ID {v_db_id} (Operation: {operation.operation.name}).")
                    if process_transcription_response(response, transcript_final_path):
                        update_video_transcription_status_db(conn, v_db_id, 'completed', transcript_path_str=transcript_final_path, completed=True)
                        add_processing_log_db(conn, v_db_id, 'transcription', 'completed', f"Transcript saved to {transcript_final_path}")
                        completed_count += 1
                    else:
                        err_msg = "Failed to process API response and save transcript."
                        update_video_transcription_status_db(conn, v_db_id, 'failed', error_msg_str=err_msg)
                        add_processing_log_db(conn, v_db_id, 'transcription', 'failed_processing_response', err_msg)
                        failed_processing_count +=1
                except Exception as e: # Catch errors from operation.result() or process_transcription_response
                    err_msg = f"Error processing result for operation {operation.operation.name}: {e}"
                    logging.error(err_msg)
                    update_video_transcription_status_db(conn, v_db_id, 'failed', error_msg_str=str(e))
                    add_processing_log_db(conn, v_db_id, 'transcription', 'failed_api_result', str(e))
                    failed_processing_count += 1
            else:
                next_active_operations.append((v_db_id, operation, transcript_final_path, gcs_blob, local_audio))
        
        active_operations = next_active_operations
        if active_operations:
            logging.info(f"{len(active_operations)} operations still pending. Waiting for 60 seconds...")
            time.sleep(60) # Polling interval

    logging.info(f"Transcription processing finished. Completed: {completed_count}, Failed: {failed_processing_count}")

    # Step 5: Cleanup
    logging.info("=== PHASE 5: Cleaning up temporary files ===")
    # Cleanup GCS files
    logging.info(f"Cleaning up {len(files_for_cleanup_gcs)} GCS files...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        gcs_cleanup_futures = [executor.submit(delete_gcs_file, current_gcs_bucket_name, blob_name) for blob_name in set(files_for_cleanup_gcs)]
        for future in concurrent.futures.as_completed(gcs_cleanup_futures):
            try:
                future.result() # We mostly care that it ran, logging is in delete_gcs_file
            except Exception as e:
                logging.warning(f"Exception during GCS cleanup future: {e}")
    
    # Cleanup local audio files
    logging.info(f"Cleaning up {len(files_for_cleanup_local)} local audio files...")
    for audio_file_path in set(files_for_cleanup_local):
        try:
            if os.path.exists(audio_file_path):
                os.remove(audio_file_path)
                logging.info(f"Deleted local audio file: {audio_file_path}")
        except Exception as e:
            logging.warning(f"Failed to delete local audio file {audio_file_path}: {e}")
            
    if conn:
        conn.close()
        logging.info("Database connection closed.")

    total_elapsed_time = time.time() - total_start_time
    logging.info(f"--- Video Transcription Script Finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    logging.info(f"Total runtime: {time.strftime('%H:%M:%S', time.gmtime(total_elapsed_time))}")
    logging.info(f"Summary: Videos initially targeted: {len(videos_to_process)}")
    logging.info(f"  Audio Extractions Successful: {len(audio_files_to_upload)}")
    logging.info(f"  GCS Uploads Successful: {len(gcs_upload_data)}")
    logging.info(f"  API Submissions Successful: {len(transcription_operations) - len(failed_submissions)}") # Initial operations list size
    logging.info(f"  Transcriptions Completed & Saved: {completed_count}")
    logging.info(f"  Transcriptions Failed (API/Processing): {failed_processing_count + len(failed_submissions) + len(failed_uploads) + len(failed_extractions)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transcribe videos using Google Cloud Speech-to-Text, with DB integration.")
    parser.add_argument(
        "--transcript-dir",
        default=TRANSCRIPT_DIR,
        help=f"Directory to save transcript files. Overrides TRANSCRIPTS_DIR env var. Default: {TRANSCRIPT_DIR}"
    )
    parser.add_argument(
        "--gcs-bucket",
        default=GCS_BUCKET_NAME,
        help=f"GCS bucket for temporary audio storage. Overrides GCS_BUCKET_NAME env var. Default: {GCS_BUCKET_NAME}"
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=None,
        help="Maximum number of videos to process in this run (optional)."
    )
    args = parser.parse_args()

    if not args.transcript_dir:
        logging.error("Transcript directory not set. Use --transcript-dir or TRANSCRIPTS_DIR env var.")
        sys.exit(1)
    if not args.gcs_bucket:
        logging.error("GCS bucket not set. Use --gcs-bucket or GCS_BUCKET_NAME env var.")
        sys.exit(1)

    main(args.transcript_dir, args.gcs_bucket, args.max_videos) 