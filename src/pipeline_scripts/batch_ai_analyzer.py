#!/usr/bin/env python3

import os
import sys
import json
import logging
import sqlite3
import concurrent.futures
import threading
import time
from typing import Dict, List, Tuple, Optional, Callable
from datetime import datetime
import google.generativeai as genai
from google.api_core.exceptions import GoogleAPIError
# from dotenv import load_dotenv # dotenv loading will be handled by the main Flet app if needed

# Import from our database manager (adjust path if necessary when used as a module)
# Assuming database_manager.py is in the same directory or accessible in PYTHONPATH
try:
    from .database_manager import create_connection
except ImportError:
    # Fallback for direct execution if needed, or if structure changes
    from database_manager import create_connection


# --- Configuration ---
LOG_FILE = "batch_analysis.log"  # Consider making this configurable or part of app's logging
SCRIPT_NAME = "batch_ai_analyzer.py" # Updated script name

# Get required paths from environment variables (these might be passed directly in a Flet app)
# ANALYSIS_DIR = os.getenv("ANALYSIS_DIR") # This might not be needed if Flet handles outputs

DEFAULT_MAX_WORKERS = 4

# --- Thread local storage for database connections ---
thread_local = threading.local()

# --- Logging Setup ---
# The Flet app might have its own logging. For now, keep this for module-level logging.
# It's good practice to allow the main application to configure logging.
logger = logging.getLogger(__name__)
if not logger.hasHandlers(): # Avoid adding handlers multiple times if imported repeatedly
    logger.setLevel(logging.INFO)
    # formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    # log_file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    # log_file_handler.setFormatter(formatter)
    # stream_handler = logging.StreamHandler()
    # stream_handler.setFormatter(formatter)
    # logger.addHandler(log_file_handler)
    # logger.addHandler(stream_handler)
    # For now, let Flet app handle logging config. If running standalone, this would be needed.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'), # Consider app-level logging dir
            logging.StreamHandler()
        ]
    )


# --- Database Functions ---
def get_db_connection(db_path: str) -> sqlite3.Connection: # db_path instead of db_name
    """Get a thread-local database connection."""
    if not hasattr(thread_local, "connections"):
        thread_local.connections = {}
        
    thread_id = threading.get_ident()
    connection_key = (thread_id, db_path)

    if connection_key not in thread_local.connections or thread_local.connections[connection_key] is None:
        connection = create_connection(db_path) # Use db_path
        thread_local.connections[connection_key] = connection
        logger.debug(f"Created new database connection for thread {thread_id} to {db_path}")
    
    return thread_local.connections[connection_key]

def close_thread_connections(db_path: str):
    """Close all database connections for the current thread for a specific DB path."""
    if hasattr(thread_local, "connections"):
        thread_id = threading.get_ident()
        connection_key = (thread_id, db_path)
        if connection_key in thread_local.connections and thread_local.connections[connection_key] is not None:
            try:
                thread_local.connections[connection_key].close()
                thread_local.connections[connection_key] = None
                logger.debug(f"Closed database connection for thread {thread_id} to {db_path}")
            except Exception as e:
                logger.warning(f"Error closing thread-local connection to {db_path}: {e}")

def get_videos_for_analysis_from_db(conn: sqlite3.Connection, limit: int = None) -> List[Dict]:
    """Get videos ready for analysis (generalized from summarization)."""
    cursor = conn.cursor()
    table_name = "videos" 

    query = f'''
    SELECT 
        v.id, 
        v.video_id, 
        v.title, 
        v.channel_id, 
        v.text_source, 
        v.plain_text_subtitle_path,
        v.segmented_10w_transcript_path
    FROM {table_name} v
    WHERE (v.analysis_status = \'pending\' OR v.analysis_status = \'failed\')
      AND (
            (v.text_source = \'SUBTITLE\' AND v.subtitle_to_text_status = \'completed\' AND v.plain_text_subtitle_path IS NOT NULL)
            OR 
            ( (v.text_source = \'TRANSCRIPTION\' OR v.text_source IS NULL)
              AND v.segmentation_10w_status = \'completed\' AND v.segmented_10w_transcript_path IS NOT NULL
            )
          )
    ORDER BY v.last_updated_at ASC
    '''    
    if limit:
        query += f' LIMIT {limit}'
    
    cursor.execute(query)
    
    results = []
    for row in cursor.fetchall():
        video_db_id, youtube_video_id, video_title, channel_id, text_source, plain_text_path, segmented_path = row
        
        source_text_file_path = None
        if text_source == 'SUBTITLE' and plain_text_path:
            source_text_file_path = plain_text_path
        elif (text_source == 'TRANSCRIPTION' or text_source is None) and segmented_path:
            source_text_file_path = segmented_path
        else:
            logger.warning(f"Video DB ID {video_db_id} matched for analysis but has inconsistent text_source ('{text_source}') or missing paths. Skipping.")
            continue

        results.append({
            'video_db_id': video_db_id,
            'youtube_video_id': youtube_video_id,
            'video_title': video_title,
            'channel_id': channel_id,
            'text_source': text_source,
            'source_text_path': source_text_file_path
        })
    
    logger.info(f"Found {len(results)} videos ready for analysis in table '{table_name}'.")
    return results

def update_video_analysis_db(
    conn: sqlite3.Connection, 
    video_db_id: int, 
    status: str,
    analysis_content: Optional[str] = None, # Renamed from summary
    error_message: Optional[str] = None,
    initiated: bool = False,
    completed: bool = False
) -> None:
    """Update the analysis for a video entry."""
    cursor = conn.cursor()
    
    update_fields = ["last_updated_at = CURRENT_TIMESTAMP"]
    params = []
    
    if analysis_content is not None:
        update_fields.append("ai_analysis_content = ?")
        params.append(analysis_content)
    
    if error_message is not None:
        update_fields.append("analysis_error_message = ?")
        params.append(error_message)
    
    update_fields.append("analysis_status = ?")
    params.append(status)
    
    if initiated:
        update_fields.append("analysis_initiated_at = CURRENT_TIMESTAMP")
    
    if completed and status == 'completed':
        update_fields.append("analysis_completed_at = CURRENT_TIMESTAMP")
    
    query = f"UPDATE videos SET {', '.join(update_fields)} WHERE id = ?"
    params.append(video_db_id)
    
    try:
        cursor.execute(query, params)
        conn.commit()
        logger.info(f"Updated video (DB ID: {video_db_id}). Status: '{status}'")
    except sqlite3.Error as e:
        logger.error(f"Database error updating video analysis (DB ID: {video_db_id}): {e}")
        logger.error(f"Query: {query}, Params: {params}")

def add_processing_log_db(
    conn: sqlite3.Connection,
    video_db_id: int,
    stage: str, # e.g., "summary_batch", "themes_batch"
    status: str,
    message: str,
    details_dict: Optional[Dict] = None
) -> None:
    """Add a log entry to the processing_logs table."""
    details_json_str = json.dumps(details_dict) if details_dict else None
    try:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO processing_logs 
        (video_record_id, stage, status, message, details, timestamp, source_script)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        ''', (video_db_id, stage, status, message, details_json_str, SCRIPT_NAME))
        
        conn.commit()
        logger.debug(f"Added log entry for video DB ID {video_db_id}: {stage} - {status}")
    except sqlite3.Error as e:
        logger.error(f"Database error adding processing log for video DB ID {video_db_id}: {e}")

# --- Gemini API Functions ---
def initialize_gemini_api(api_key: str) -> bool:
    """Initialize the Gemini API client. Returns True on success, False on failure."""
    try:
        genai.configure(api_key=api_key)
        logger.info("Initialized Gemini API client")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Gemini API: {e}")
        return False


def perform_ai_analysis_on_transcript( # Renamed from summarize_transcript
    video_info: Dict,
    api_key: str, # Should be passed, not read from env here
    db_path: str,
    prompt_template_str: str, # New argument for the prompt
    stage_name: str = "ai_analysis_batch" # For logging
) -> Dict:
    """Perform AI analysis on a single transcript using Gemini with a given prompt."""
    conn = get_db_connection(db_path)
    
    video_db_id = video_info['video_db_id']
    video_title = video_info['video_title']
    source_text_path = video_info['source_text_path']
    text_source_type = video_info['text_source']
    
    result = {
        'video_db_id': video_db_id,
        'video_title': video_title,
        'success': False,
        'analysis_content': None,
        'error': None
    }
    
    try:
        update_video_analysis_db(
            conn, 
            video_db_id, 
            "analyzing", # Generic status
            initiated=True
        )
        
        add_processing_log_db(
            conn,
            video_db_id,
            stage=stage_name,
            status="initiated",
            message=f"Starting AI analysis (source: {text_source_type if text_source_type else 'transcription'}) for video: {video_title}"
        )
        
        if not source_text_path or not os.path.exists(source_text_path):
            error_msg = f"Source text file not found: {source_text_path}"
            logger.error(error_msg)
            update_video_analysis_db(conn, video_db_id, "failed", error_message=error_msg)
            add_processing_log_db(conn, video_db_id, stage=stage_name, status="error", message=error_msg)
            result['error'] = error_msg
            return result
        
        with open(source_text_path, 'r', encoding='utf-8') as f:
            transcript_content = f.read()
        
        add_processing_log_db(
            conn, video_db_id, stage=stage_name, status="transcript_loaded",
            message=f"Transcript content loaded ({len(transcript_content)} characters)"
        )
        
        # Prompt is now constructed using the template
        # Ensure {transcript_content} and potentially {video_title} are placeholders
        prompt = prompt_template_str.format(transcript_content=transcript_content, video_title=video_title)
            
        # Model initialization should happen once, or be more robust.
        # For batch, initializing per call is okay but consider efficiency for many short calls.
        # Ensure API key is valid before this step
        # initialize_gemini_api(api_key) # This should be done once before starting the batch.

        model = genai.GenerativeModel('gemini-2.0-flash') # Changed to 2.0 flash
                                                       
        
        add_processing_log_db(
            conn, video_db_id, stage=stage_name, status="api_request_sent",
            message="Transcript sent to Gemini API for analysis"
        )
            
        response = model.generate_content(prompt)
        analysis_text = response.text.strip()
            
        update_video_analysis_db(
            conn, video_db_id, "completed", 
            analysis_content=analysis_text,
            completed=True
        )
        add_processing_log_db(
            conn, video_db_id, stage=stage_name, status="success",
            message=f"AI Analysis completed for video: {video_title}",
            details_dict={"analysis_length": len(analysis_text)}
        )
        result['success'] = True
        result['analysis_content'] = analysis_text
        return result
                
    except GoogleAPIError as e:
        error_msg = f"Gemini API error: {str(e)}"
        logger.error(error_msg)
        update_video_analysis_db(conn, video_db_id, "failed", error_message=error_msg)
        add_processing_log_db(conn, video_db_id, stage=stage_name, status="error", message=error_msg)
        result['error'] = error_msg
        return result
        
    except Exception as e:
        error_msg = f"Unexpected error during AI analysis for video {video_db_id}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        update_video_analysis_db(conn, video_db_id, "failed", error_message=error_msg)
        add_processing_log_db(conn, video_db_id, stage=stage_name, status="error", message=error_msg)
        result['error'] = error_msg
        return result
    finally:
        close_thread_connections(db_path)


def run_batch_analysis( # Renamed from process_transcripts_for_summarization
    db_path: str,
    api_key: str,
    prompt_template_str: str,
    prompt_key_for_logging: str, # e.g., "summary" or "themes" for the stage name
    max_workers: int = DEFAULT_MAX_WORKERS,
    max_videos: Optional[int] = None,
    progress_callback: Optional[Callable[[int, int, int, int], None]] = None # (processed, total, success, failed)
) -> Dict[str, int]:
    """Process transcripts in batch using a specific prompt."""

    if not api_key:
        logger.error("API key not provided for batch analysis.")
        return {"total_processed": 0, "successful": 0, "failed": 0, "error": "API Key missing"}

    if not initialize_gemini_api(api_key): # Initialize API once before starting threads
        logger.error("Failed to initialize Gemini API. Batch analysis aborted.")
        return {"total_processed": 0, "successful": 0, "failed": 0, "error": "API Initialization failed"}

    conn = get_db_connection(db_path)
    videos_to_analyze = get_videos_for_analysis_from_db(conn, limit=max_videos)
    close_thread_connections(db_path) # Close main thread's connection before starting pool

    if not videos_to_analyze:
        logger.info("No videos found requiring AI analysis for the current criteria.")
        return {"total_processed": 0, "successful": 0, "failed": 0}
    
    logger.info(f"Found {len(videos_to_analyze)} videos for AI analysis using prompt key: {prompt_key_for_logging}")

    total_processed = 0
    successful_analyses = 0
    failed_analyses = 0
    
    stage_log_name = f"{prompt_key_for_logging}_batch_analysis"

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_video = {
            executor.submit(perform_ai_analysis_on_transcript, video_info, api_key, db_path, prompt_template_str, stage_log_name):
            video_info for video_info in videos_to_analyze
        }
        
        for future in concurrent.futures.as_completed(future_to_video):
            video_info = future_to_video[future]
            video_db_id = video_info['video_db_id']
            
            try:
                result = future.result()
                total_processed += 1
                
                if result['success']:
                    successful_analyses += 1
                    logger.info(f"Successfully analyzed video {video_db_id} ({result.get('video_title', 'N/A')})")
                else:
                    failed_analyses += 1
                    logger.error(f"Failed to analyze video {video_db_id} ({result.get('video_title', 'N/A')}) - {result['error']}")
                
                if progress_callback:
                    progress_callback(total_processed, len(videos_to_analyze), successful_analyses, failed_analyses)
                
            except Exception as e:
                failed_analyses += 1
                total_processed += 1
                logger.error(f"Exception processing future for video {video_db_id}: {e}", exc_info=True)
                if progress_callback:
                    progress_callback(total_processed, len(videos_to_analyze), successful_analyses, failed_analyses)

    logger.info(f"Batch AI analysis ({prompt_key_for_logging}) complete. "
                f"Processed {total_processed}/{len(videos_to_analyze)}. "
                f"Success: {successful_analyses} | Failed: {failed_analyses}")
    
    return {"total_processed": total_processed, "successful": successful_analyses, "failed": failed_analyses}

# Example of how this module could be called if needed (for testing, not direct execution)
# if __name__ == '__main__':
#     print("This script is intended to be used as a module.")
    # # Example:
    # TEST_DB_PATH = "test_db.sqlite3" # Replace with your test DB
    # TEST_API_KEY = "YOUR_GEMINI_API_KEY" # Replace
    # 
    # # Make sure database_manager.py and a test DB exist for this example
    # if not os.path.exists(TEST_DB_PATH):
    #     print(f"Test database {TEST_DB_PATH} not found. Skipping example.")
    # else:
    #     # Example prompt (use one from ai_core.PROMPTS usually)
    #     example_summary_prompt = \"\"\"Summarize: {transcript_content}\"\"\"
    # 
    #     def my_progress_callback(processed, total, success, failed):
    #         print(f"Progress: {processed}/{total} | Success: {success} | Failed: {failed}")
    # 
    #     results = run_batch_analysis(
    #         db_path=TEST_DB_PATH,
    #         api_key=TEST_API_KEY,
    #         prompt_template_str=example_summary_prompt,
    #         prompt_key_for_logging="test_summary",
    #         max_videos=5, # Process up to 5 videos
    #         progress_callback=my_progress_callback
    #     )
    #     print(f"Batch analysis results: {results}") 