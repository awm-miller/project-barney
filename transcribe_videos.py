#!/usr/bin/env python3

import os
import argparse
import json
import logging
import subprocess
import time
import sys
import uuid
import concurrent.futures
from datetime import datetime
from google.cloud import speech
from google.cloud import storage
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()

# --- Configuration ---
LOG_FILE = "transcribe_videos.log"

# Get required paths and bucket name from environment variables
VIDEO_DIR = os.getenv("DOWNLOAD_DIR") # Source videos from the download directory
if not VIDEO_DIR:
    raise ValueError("DOWNLOAD_DIR not found in .env file. This is needed to find downloaded videos.")

TRANSCRIPT_DIR = os.getenv("TRANSCRIPTS_DIR")
if not TRANSCRIPT_DIR:
    raise ValueError("TRANSCRIPTS_DIR not found in .env file. Please set it to your desired transcript output path.")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
if not GCS_BUCKET_NAME:
    raise ValueError("GCS_BUCKET_NAME not found in .env file. Please set it to your Google Cloud Storage bucket name.")

# DEFAULT_VIDEO_DIR = r"E:\hate-preachers\test\downloads" # Removed hardcoded default
# DEFAULT_TRANSCRIPT_DIR = "transcripts" # Removed hardcoded default
DEFAULT_MAPPING_FILE = "video_transcript_mapping.json" # Keep default for this less critical file
DEFAULT_AUDIO_FORMAT = "flac" # Preferred format for GCP Speech-to-Text
# DEFAULT_GCS_BUCKET = "video-transcription-bucket123" # Removed hardcoded default
MAX_DIRECT_API_SIZE = 10 * 1024 * 1024  # 10MB
MAX_WORKERS = 4  # Maximum number of parallel uploads/downloads
UPLOAD_TIMEOUT = 1800  # 30 minutes timeout for large file uploads
DATABASE_PATH = os.getenv("DATABASE_PATH") # Need DB path for initialization
if not DATABASE_PATH:
    raise ValueError("DATABASE_PATH not found in .env file. Please set it to your desired database file path.")

# --- Logging Setup ---
# Configure file handler with UTF-8 encoding
file_handler = logging.FileHandler(LOG_FILE, 'a', 'utf-8')

# Configure stream handler that can handle unicode
class UnicodeStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            # Ensure unicode compatibility
            stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

# Set up the root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        file_handler,
        UnicodeStreamHandler(sys.stdout)
    ]
)

# --- Helper Functions ---

def ensure_dir_exists(directory_path: str):
    """Creates a directory if it doesn't exist."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logging.info(f"Created directory: {directory_path}")

def sanitize_filename(name: str) -> str:
    """Sanitize string to be safe for filenames."""
    return "".join(c if c.isalnum() or c in ' ._-' else '_' for c in name).strip()

def find_video_files(directory: str, extensions=('.mp4', '.avi', '.mov', '.mkv', '.webm')) -> list[str]:
    """Finds video files with specified extensions in a directory."""
    video_files = []
    logging.info(f"Searching for video files in: {directory}")
    try:
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            if os.path.isfile(item_path) and item.lower().endswith(extensions):
                video_files.append(item_path)
    except FileNotFoundError:
        logging.error(f"Video directory not found: {directory}")
        return []
    except Exception as e:
        logging.error(f"Error listing video directory {directory}: {e}")
        return []
    logging.info(f"Found {len(video_files)} video files.")
    return video_files

def extract_audio(video_path: str, audio_path: str) -> bool:
    """
    Extracts audio from video file using ffmpeg.
    Returns True on success, False on failure.
    Requires ffmpeg to be installed and in the system PATH.
    """
    logging.info(f"Extracting audio from '{os.path.basename(video_path)}' to '{os.path.basename(audio_path)}'")
    command = [
        'ffmpeg',
        '-i', video_path,      # Input file
        '-vn',                 # Disable video recording
        '-acodec', 'flac' if DEFAULT_AUDIO_FORMAT == 'flac' else 'pcm_s16le', # Audio codec (FLAC or WAV)
        '-ar', '16000',        # Audio sample rate (16kHz recommended for speech)
        '-ac', '1',            # Mono channel
        '-y',                  # Overwrite output file if it exists
        audio_path
    ]
    try:
        process = subprocess.run(command, check=True, capture_output=True, text=True)
        logging.debug(f"ffmpeg stdout: {process.stdout}")
        logging.debug(f"ffmpeg stderr: {process.stderr}")
        logging.info(f"Successfully extracted audio for: {os.path.basename(video_path)}")
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

def upload_audio_to_gcs(audio_path: str, gcs_bucket: str, job_folder: str) -> tuple[str, str, bool]:
    """
    Uploads an audio file to Google Cloud Storage.
    Returns (gcs_uri, gcs_blob_name, success_flag).
    Uses job_folder to organize files in GCS bucket.
    """
    # Keep original filename but sanitize it for GCS
    audio_basename = sanitize_filename(os.path.basename(audio_path))
    gcs_blob_name = f"{job_folder}/{audio_basename}"
    gcs_uri = f"gs://{gcs_bucket}/{gcs_blob_name}"
    
    try:
        storage_client = storage.Client()
        
        # Get the bucket
        try:
            bucket = storage_client.get_bucket(gcs_bucket)
        except NotFound:
            logging.info(f"Bucket {gcs_bucket} not found, creating it...")
            bucket = storage_client.create_bucket(gcs_bucket)
            
        blob = bucket.blob(gcs_blob_name)
        
        logging.info(f"Uploading {audio_basename} to Google Cloud Storage: {gcs_uri}")
        logging.info(f"File size: {os.path.getsize(audio_path) / (1024*1024):.2f} MB")
        
        # Upload the file with a timeout
        upload_start = time.time()
        
        # Set up explicit timeout for large files
        blob.upload_from_filename(
            audio_path,
            timeout=UPLOAD_TIMEOUT  # Extended timeout for large files
        )
        
        upload_duration = time.time() - upload_start
        upload_speed = os.path.getsize(audio_path) / (1024*1024) / upload_duration if upload_duration > 0 else 0
        logging.info(f"Upload of {audio_basename} completed in {upload_duration:.2f}s ({upload_speed:.2f} MB/s)")
        
        return gcs_uri, gcs_blob_name, True
        
    except Exception as e:
        logging.error(f"Error uploading {audio_basename} to GCS: {e}")
        return None, gcs_blob_name, False

def delete_gcs_file(gcs_bucket: str, gcs_blob_name: str) -> bool:
    """Deletes a file from GCS bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(gcs_bucket)
        blob = bucket.blob(gcs_blob_name)
        blob.delete()
        logging.info(f"Deleted GCS file: {gcs_blob_name}")
        return True
    except Exception as e:
        logging.warning(f"Failed to delete GCS file {gcs_blob_name}: {e}")
        return False

def process_transcription_response(response, transcript_path: str) -> bool:
    """Process transcription response and write results to file."""
    try:
        transcript_content = []
        # Iterate through speech segments (results) provided by the API
        for result in response.results:
            # Ensure there are alternatives and grab the best one
            if result.alternatives:
                best_alternative = result.alternatives[0]
                # Append the transcript text for this segment
                transcript_content.append(best_alternative.transcript.strip())

        # Write the transcript to file, with each segment on a new line
        with open(transcript_path, "w", encoding="utf-8") as f:
            f.write("\\n".join(transcript_content))

        logging.info(f"Successfully processed and saved transcript: {transcript_path}") # Added log
        return True
    except Exception as e:
        logging.error(f"Error processing transcription response for {transcript_path}: {e}")
        # Log the response structure might help debugging if needed
        # logging.debug(f"Transcription Response structure: {response}")
        return False

# --- Main Execution ---

def main(video_dir: str, transcript_dir: str, mapping_file: str, gcs_bucket: str, db_path: str):
    """Main workflow for transcribing videos."""
    total_start_time = time.time()
    start_datetime = datetime.now()
    logging.info(f"--- Starting Video Transcription Script at {start_datetime.strftime('%Y-%m-%d %H:%M:%S')} ---")

    ensure_dir_exists(transcript_dir)
    initialize_database(db_path)

    # Find video files
    video_files = find_video_files(video_dir)
    if not video_files:
        logging.warning("No video files found. Exiting.")
        return

    # Create a unique job ID folder in GCS
    job_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_folder = f"transcription_job_{job_id}"
    logging.info(f"Created job folder in GCS: {job_folder}")

    # Initialize tracking lists/dicts
    results_mapping = []
    success_count = 0
    fail_count = 0
    
    # Process data structures
    audio_extractions = []  # List of (video_path, audio_path) tuples
    gcs_uploads = []        # List of (audio_path, gcs_uri, gcs_blob_name) tuples
    transcription_ops = []  # List of (audio_basename, operation, transcript_path, gcs_blob_name) tuples
    
    # Step 1: Extract audio from all videos (serial process)
    logging.info(f"=== PHASE 1: Extracting audio from {len(video_files)} videos ===")
    for video_path in video_files:
        base_name = os.path.splitext(os.path.basename(video_path))[0]
        safe_base_name = sanitize_filename(base_name)
        audio_filename = f"{safe_base_name}.{DEFAULT_AUDIO_FORMAT}"
        audio_path = os.path.join(transcript_dir, audio_filename)
        
        if extract_audio(video_path, audio_path):
            audio_extractions.append((video_path, audio_path))
        else:
            # Record failed extraction
            results_mapping.append({
                "video_path": os.path.abspath(video_path),
                "transcript_path": None,
                "status": "failed",
                "error": "Audio extraction failed"
            })
            fail_count += 1
    
    logging.info(f"Successfully extracted audio from {len(audio_extractions)} of {len(video_files)} videos")
    
    if not audio_extractions:
        logging.error("No audio files were successfully extracted. Exiting.")
        return
    
    # Step 2: Upload all audio files to GCS in parallel
    logging.info(f"=== PHASE 2: Uploading {len(audio_extractions)} audio files to GCS ===")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Start the upload operations and mark each future with its audio_path
        future_to_audio = {
            executor.submit(upload_audio_to_gcs, audio_path, gcs_bucket, job_folder): (video_path, audio_path)
            for video_path, audio_path in audio_extractions
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_audio):
            video_path, audio_path = future_to_audio[future]
            try:
                gcs_uri, gcs_blob_name, success = future.result()
                if success:
                    gcs_uploads.append((video_path, audio_path, gcs_uri, gcs_blob_name))
                else:
                    # Record failed upload
                    results_mapping.append({
                        "video_path": os.path.abspath(video_path),
                        "transcript_path": None,
                        "status": "failed",
                        "error": "GCS upload failed"
                    })
                    fail_count += 1
            except Exception as e:
                logging.error(f"Exception while uploading {os.path.basename(audio_path)}: {e}")
                results_mapping.append({
                    "video_path": os.path.abspath(video_path),
                    "transcript_path": None,
                    "status": "failed", 
                    "error": f"Upload exception: {str(e)}"
                })
                fail_count += 1
    
    logging.info(f"Successfully uploaded {len(gcs_uploads)} of {len(audio_extractions)} audio files to GCS")
    
    if not gcs_uploads:
        logging.error("No audio files were successfully uploaded to GCS. Exiting.")
        return
    
    # Step 3: Submit all transcription requests
    logging.info(f"=== PHASE 3: Submitting {len(gcs_uploads)} transcription requests ===")
    
    speech_client = speech.SpeechClient()
    for video_path, audio_path, gcs_uri, gcs_blob_name in gcs_uploads:
        try:
            # Prepare transcript path
            base_name = os.path.splitext(os.path.basename(video_path))[0]
            safe_base_name = sanitize_filename(base_name)
            transcript_filename = f"{safe_base_name}_transcript.txt"
            transcript_path = os.path.join(transcript_dir, transcript_filename)
            
            # Configure transcription
            config = speech.RecognitionConfig(
                encoding=speech.RecognitionConfig.AudioEncoding.FLAC if DEFAULT_AUDIO_FORMAT == 'flac' else speech.RecognitionConfig.AudioEncoding.LINEAR16,
                sample_rate_hertz=16000,
                language_code="en-US",
                enable_word_time_offsets=True,
            )
            
            # Create the request
            audio = speech.RecognitionAudio(uri=gcs_uri)
            
            # Submit the request
            logging.info(f"Submitting transcription request for {os.path.basename(audio_path)}")
            operation = speech_client.long_running_recognize(config=config, audio=audio)
            
            # Store operation for later polling
            transcription_ops.append((
                os.path.basename(audio_path),
                operation,
                transcript_path,
                video_path,
                gcs_blob_name
            ))
            
        except Exception as e:
            logging.error(f"Failed to submit transcription for {os.path.basename(audio_path)}: {e}")
            results_mapping.append({
                "video_path": os.path.abspath(video_path),
                "transcript_path": None,
                "status": "failed",
                "error": f"Transcription submission error: {str(e)}"
            })
            fail_count += 1
    
    logging.info(f"Submitted {len(transcription_ops)} transcription operations")
    
    # Step 4: Poll operations and process results
    logging.info(f"=== PHASE 4: Waiting for {len(transcription_ops)} transcription operations to complete ===")
    logging.info("This may take a while depending on the length and number of audio files")
    
    transcription_in_progress = transcription_ops.copy()
    completed_ops = []
    
    # Poll until all operations complete
    poll_interval = 300  # seconds between polling attempts
    
    while transcription_in_progress:
        logging.info(f"Checking {len(transcription_in_progress)} operations in progress...")
        still_in_progress = []
        
        for audio_basename, operation, transcript_path, video_path, gcs_blob_name in transcription_in_progress:
            if operation.done():
                try:
                    # Get the result
                    response = operation.result()
                    logging.info(f"Transcription completed for {audio_basename}")
                    
                    # Process and save the transcript
                    if process_transcription_response(response, transcript_path):
                        # Record success
                        results_mapping.append({
                            "video_path": os.path.abspath(video_path),
                            "transcript_path": os.path.abspath(transcript_path),
                            "gcs_uri": f"gs://{gcs_bucket}/{gcs_blob_name}",
                            "job_id": job_id,
                            "status": "success"
                        })
                        success_count += 1
                        completed_ops.append((video_path, transcript_path, gcs_blob_name))
                    else:
                        # Record processing failure
                        results_mapping.append({
                            "video_path": os.path.abspath(video_path),
                            "transcript_path": None,
                            "gcs_uri": f"gs://{gcs_bucket}/{gcs_blob_name}",
                            "job_id": job_id,
                            "status": "failed",
                            "error": "Failed to process transcription response"
                        })
                        fail_count += 1
                except Exception as e:
                    logging.error(f"Error processing transcription for {audio_basename}: {e}")
                    results_mapping.append({
                        "video_path": os.path.abspath(video_path),
                        "transcript_path": None,
                        "gcs_uri": f"gs://{gcs_bucket}/{gcs_blob_name}",
                        "job_id": job_id,
                        "status": "failed",
                        "error": f"Transcription processing error: {str(e)}"
                    })
                    fail_count += 1
            else:
                # Operation still in progress
                still_in_progress.append((audio_basename, operation, transcript_path, video_path, gcs_blob_name))
        
        # Update the in-progress list
        transcription_in_progress = still_in_progress
        
        # If there are still operations pending, wait before checking again
        if transcription_in_progress:
            logging.info(f"Waiting for {len(transcription_in_progress)} operations to complete...")
            logging.info(f"Progress: {len(completed_ops)}/{len(transcription_ops)} files transcribed")
            time.sleep(poll_interval)
    
    # Step 5: Clean up GCS files (optional)
    logging.info(f"=== PHASE 5: Cleaning up {len(completed_ops)} GCS files ===")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Start the delete operations
        future_to_filename = {
            executor.submit(delete_gcs_file, gcs_bucket, gcs_blob_name): gcs_blob_name
            for _, _, gcs_blob_name in completed_ops
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
    
    # Step 6: Clean up local audio files
    logging.info("=== PHASE 6: Cleaning up local temporary audio files ===")
    for _, audio_path in audio_extractions:
        try:
            if os.path.exists(audio_path):
                os.remove(audio_path)
        except Exception as e:
            logging.warning(f"Failed to remove temporary audio file {audio_path}: {e}")
    
    # Write the mapping file
    logging.info(f"Writing mapping data for {len(results_mapping)} videos to {mapping_file}")
    try:
        mapping_data = {
            "job_id": job_id,
            "job_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "gcs_bucket": gcs_bucket,
            "gcs_job_folder": job_folder,
            "video_count": len(video_files),
            "success_count": success_count,
            "fail_count": fail_count,
            "files": results_mapping
        }
        
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, indent=4)
        logging.info(f"Successfully wrote mapping file: {mapping_file}")
    except Exception as e:
        logging.error(f"Failed to write mapping file {mapping_file}: {e}")
    
    # Final Summary
    end_script_time = time.time()
    total_elapsed = end_script_time - total_start_time
    logging.info(f"--- Video Transcription Script Finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    logging.info(f"Total runtime: {time.strftime('%H:%M:%S', time.gmtime(total_elapsed))} ({total_elapsed:.2f} seconds)")
    logging.info(f"Total videos processed: {len(video_files)}")
    logging.info(f"Successful transcriptions: {success_count}")
    logging.info(f"Failed transcriptions: {fail_count}")
    logging.info(f"Job ID: {job_id}")
    logging.info(f"GCS Job Folder: {job_folder}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract audio, transcribe videos using Google Cloud Speech-to-Text, and update database.")
    parser.add_argument(
        "--video-dir",
        default=VIDEO_DIR, # Default comes from env var
        help="Directory containing video files to process. Overrides DOWNLOAD_DIR environment variable."
    )
    parser.add_argument(
        "--transcript-dir",
        default=TRANSCRIPT_DIR, # Default comes from env var
        help="Directory to save transcript files. Overrides TRANSCRIPTS_DIR environment variable."
    )
    parser.add_argument(
        "--mapping-file",
        default=DEFAULT_MAPPING_FILE,
        help=f"Path to save the video-to-transcript mapping JSON file. Default: {DEFAULT_MAPPING_FILE}"
    )
    parser.add_argument(
        "--gcs-bucket",
        default=GCS_BUCKET_NAME, # Default comes from env var
        help="Google Cloud Storage bucket name for large audio files. Overrides GCS_BUCKET_NAME environment variable."
    )
    parser.add_argument(
        "--db-path",
        default=DATABASE_PATH, # Default comes from env var
        help="Path to the SQLite database file. Overrides DATABASE_PATH environment variable."
    )

    args = parser.parse_args()

    main(
        args.video_dir,
        args.transcript_dir,
        args.mapping_file,
        args.gcs_bucket,
        args.db_path
    ) 