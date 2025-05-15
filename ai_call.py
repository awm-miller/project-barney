#!/usr/bin/env python3

import os
import sys
import json
import logging
import argparse
import sqlite3
import concurrent.futures
import threading
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import google.generativeai as genai
from google.api_core.exceptions import GoogleAPIError
from dotenv import load_dotenv

# Import from our database manager
from database_manager import create_connection, DATABASE_NAME

# --- Load environment variables ---
load_dotenv()

# --- Configuration ---
LOG_FILE = "summarize_transcripts.log"
SCRIPT_NAME = "summarize_transcripts.py"

# Get required paths from environment variables
ANALYSIS_DIR = os.getenv("ANALYSIS_DIR")
if not ANALYSIS_DIR:
    raise ValueError("ANALYSIS_DIR not found in .env file. Please set it to your desired analysis output path.")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in .env file.")

DEFAULT_MAX_WORKERS = 4

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
def get_db_connection() -> sqlite3.Connection:
    """Get a thread-local database connection."""
    if not hasattr(thread_local, "connections"):
        thread_local.connections = {}
        
    thread_id = threading.get_ident()
    if thread_id not in thread_local.connections or thread_local.connections[thread_id] is None:
        # Create a new connection for this thread
        connection = create_connection(DATABASE_NAME)
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

def get_videos_for_summarization_from_db(conn: sqlite3.Connection, limit: int = None, job_name: Optional[str] = None) -> List[Dict]:
    """Get videos ready for summarization, from either subtitles or transcriptions."""
    cursor = conn.cursor()
    table_name = "videos" # Always use the main videos table

    # Path 1: Completed plain text subtitle conversion
    # Path 2: Completed 10-word segmentation of a GCS transcription
    query = f'''
    SELECT 
        v.id, 
        v.video_id, 
        v.title, 
        v.channel_id, 
        v.text_source, 
        v.plain_text_subtitle_path,    -- Path if text_source is SUBTITLE
        v.segmented_10w_transcript_path -- Path if text_source is TRANSCRIPTION
    FROM {table_name} v
    WHERE (v.analysis_status = \'pending\' OR v.analysis_status = \'failed\')
      AND (
            (v.text_source = \'SUBTITLE\' AND v.subtitle_to_text_status = \'completed\' AND v.plain_text_subtitle_path IS NOT NULL)
            OR 
            ( (v.text_source = \'TRANSCRIPTION\' OR v.text_source IS NULL) -- IS NULL for legacy
              AND v.segmentation_10w_status = \'completed\' AND v.segmented_10w_transcript_path IS NOT NULL
            )
          )
    ORDER BY v.last_updated_at ASC -- Process videos that haven't been touched recently first, or by added_at
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
        elif (text_source == 'TRANSCRIPTION' or text_source is None) and segmented_path: # text_source IS NULL for legacy cases
            source_text_file_path = segmented_path
        else:
            logging.warning(f"Video DB ID {video_db_id} matched for summarization but has inconsistent text_source ('{text_source}') or missing paths. Skipping.")
            continue

        results.append({
            'video_db_id': video_db_id,
            'youtube_video_id': youtube_video_id,
            'video_title': video_title,
            'channel_id': channel_id,
            'text_source': text_source, # Important for knowing how the text was derived
            'source_text_path': source_text_file_path # Unified path to the text to be summarized
        })
    
    logging.info(f"Found {len(results)} videos ready for summarization (from subtitles or transcriptions) in table '{table_name}'.")
    return results

def update_video_summary_db(
    conn: sqlite3.Connection, 
    video_db_id: int, 
    status: str,
    summary: Optional[str] = None,
    error_message: Optional[str] = None,
    initiated: bool = False,
    completed: bool = False
) -> None:
    """Update the summary for a video entry."""
    cursor = conn.cursor()
    
    update_fields = ["last_updated_at = CURRENT_TIMESTAMP"]
    params = []
    
    if summary is not None:
        update_fields.append("ai_analysis_content = ?")
        params.append(summary)
    
    if error_message is not None:
        update_fields.append("analysis_error_message = ?")
        params.append(error_message)
    
    # Track summarization status in the analysis_status field
    update_fields.append("analysis_status = ?")
    params.append(status)
    
    # Handle timestamps conditionally
    if initiated:
        update_fields.append("analysis_initiated_at = CURRENT_TIMESTAMP")
    
    if completed and status == 'completed':
        update_fields.append("analysis_completed_at = CURRENT_TIMESTAMP")
    
    query = f"UPDATE videos SET {', '.join(update_fields)} WHERE id = ?"
    params.append(video_db_id)
    
    try:
        cursor.execute(query, params)
        conn.commit()
        logging.info(f"Updated video (DB ID: {video_db_id}) with summary. Status: '{status}'")
    except sqlite3.Error as e:
        logging.error(f"Database error updating video summary (DB ID: {video_db_id}): {e}")
        logging.error(f"Query: {query}, Params: {params}")

def add_processing_log_db(
    conn: sqlite3.Connection,
    video_db_id: int,
    stage: str,
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
        logging.debug(f"Added log entry for video DB ID {video_db_id}: {stage} - {status}")
    except sqlite3.Error as e:
        logging.error(f"Database error adding processing log for video DB ID {video_db_id}: {e}")

# --- Gemini API Functions ---
def initialize_gemini_api(api_key: str) -> None:
    """Initialize the Gemini API client."""
    genai.configure(api_key=api_key)
    logging.info("Initialized Gemini API client")

def summarize_transcript(
    video_info: Dict,
    api_key: str
) -> Dict:
    """Summarize a single transcript using Gemini."""
    # Create a thread-local database connection
    conn = get_db_connection()
    
    video_db_id = video_info['video_db_id']
    youtube_video_id = video_info['youtube_video_id'] 
    video_title = video_info['video_title']
    source_text_path = video_info['source_text_path']
    text_source_type = video_info['text_source']
    
    result = {
        'video_db_id': video_db_id,
        'video_title': video_title,
        'success': False,
        'summary': None,
        'error': None
    }
    
    try:
        # Update status to analyzing before starting
        update_video_summary_db(
            conn, 
            video_db_id, 
            "summarizing",
            initiated=True
        )
        
        add_processing_log_db(
            conn,
            video_db_id,
            stage="summary",
            status="initiated",
            message=f"Starting summarization of text (source: {text_source_type if text_source_type else 'transcription'}) for video: {video_title}"
        )
        
        # Check if transcript file exists
        if not source_text_path or not os.path.exists(source_text_path):
            error_msg = f"Source text file not found: {source_text_path}"
            logging.error(error_msg)
            update_video_summary_db(
                conn, 
                video_db_id, 
                "failed", 
                error_message=error_msg
            )
            add_processing_log_db(
                conn,
                video_db_id,
                stage="summary",
                status="error",
                message=error_msg
            )
            result['error'] = error_msg
            return result
        
        # Read transcript content
        try:
            with open(source_text_path, 'r', encoding='utf-8') as f:
                transcript_content = f.read()
            
            add_processing_log_db(
                conn,
                video_db_id,
                stage="summary",
                status="transcript_loaded",
                message=f"Transcript content loaded ({len(transcript_content)} characters)"
            )
            
            # Initialize Gemini API
            initialize_gemini_api(api_key)
            
            # Create prompt for summarization
            prompt = f"""
You are an expert linguist and religious content analyst.

TASK
Summarize the following Arabic transcript of a TV show with multiple hosts.
The video title is: "{video_title}"

TRANSCRIPT:
{transcript_content}

Please provide a concise English summary of this content in under 200 words. Focus on the main themes, arguments, and significant points made on the show. For any particularly controversial statements, include a timestamp and then a guess at who might be speaking. If it's not clear, don't guess and only include the timestamp. 

Your response should be ONLY the plain text summary with no additional formatting, headings, or explanations.
If the transcript is empty, unclear, or doesn't contain enough content to summarize, simply state that briefly.
"""
            
            # Create Gemini model client
            model = genai.GenerativeModel('gemini-2.0-flash')
            
            # Generate summary
            add_processing_log_db(
                conn,
                video_db_id,
                stage="summary",
                status="api_request_sent",
                message="Transcript sent to Gemini API for summarization"
            )
            
            response = model.generate_content(prompt)
            summary_text = response.text.strip()
            
            # Process the summary response
            try:
                # Store the summary text directly in the database
                update_video_summary_db(
                    conn, 
                    video_db_id, 
                    "completed", 
                    summary=summary_text,
                    completed=True
                )
                
                add_processing_log_db(
                    conn,
                    video_db_id,
                    stage="summary",
                    status="success",
                    message=f"Summarization completed for video: {video_title}",
                    details_dict={"summary_length": len(summary_text)}
                )
                
                result['success'] = True
                result['summary'] = summary_text
                return result
                
            except Exception as e:
                error_msg = f"Failed to process summary response: {str(e)}"
                logging.error(error_msg)
                logging.error(f"Raw response: {summary_text}")
                
                update_video_summary_db(
                    conn, 
                    video_db_id, 
                    "failed", 
                    error_message=f"Processing error: {str(e)}"
                )
                
                add_processing_log_db(
                    conn,
                    video_db_id,
                    stage="summary",
                    status="error",
                    message=f"Failed to process summary response: {str(e)}",
                    details_dict={"raw_response_snippet": summary_text[:200] + "..." if len(summary_text) > 200 else summary_text}
                )
                
                result['error'] = error_msg
                return result
                
        except Exception as e:
            error_msg = f"Error reading transcript file {source_text_path}: {str(e)}"
            logging.error(error_msg)
            update_video_summary_db(
                conn, 
                video_db_id, 
                "failed", 
                error_message=error_msg
            )
            add_processing_log_db(
                conn,
                video_db_id,
                stage="summary",
                status="error",
                message=error_msg
            )
            result['error'] = error_msg
            return result
            
    except GoogleAPIError as e:
        error_msg = f"Gemini API error: {str(e)}"
        logging.error(error_msg)
        
        update_video_summary_db(
            conn, 
            video_db_id, 
            "failed", 
            error_message=f"Gemini API error: {str(e)}"
        )
        
        add_processing_log_db(
            conn,
            video_db_id,
            stage="summary",
            status="error",
            message=f"Gemini API error: {str(e)}"
        )
        
        result['error'] = error_msg
        return result
        
    except Exception as e:
        error_msg = f"Unexpected error during summarization: {str(e)}"
        logging.error(error_msg, exc_info=True)
        
        update_video_summary_db(
            conn, 
            video_db_id, 
            "failed", 
            error_message=f"Unexpected error: {str(e)}"
        )
        
        add_processing_log_db(
            conn,
            video_db_id,
            stage="summary",
            status="error",
            message=f"Unexpected error during processing: {str(e)}"
        )
        
        result['error'] = error_msg
        return result
    finally:
        # Close the thread-local connection
        close_thread_connections()

def process_transcripts_for_summarization(
    max_workers: int = DEFAULT_MAX_WORKERS,
    api_key: str = None,
    max_videos: int = None
) -> None:
    """Process transcripts individually for summarization using parallel workers."""
    if not api_key:
        api_key = GEMINI_API_KEY
        
    # Get database connection for main thread
    conn = get_db_connection()
    
    # Get all transcripts that need summarization
    transcripts_to_process = get_videos_for_summarization_from_db(conn, limit=max_videos)
    logging.info(f"Found {len(transcripts_to_process)} transcripts for summarization")
    
    if not transcripts_to_process:
        logging.info("No transcripts to summarize")
        close_thread_connections()
        return
    
    # Create thread pool for parallel processing
    total_processed = 0
    successful_summaries = 0
    failed_summaries = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit summarization jobs for each transcript individually
        future_to_transcript = {
            executor.submit(summarize_transcript, transcript_info, api_key): 
            transcript_info for transcript_info in transcripts_to_process
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_transcript):
            transcript_info = future_to_transcript[future]
            video_db_id = transcript_info['video_db_id']
            video_title = transcript_info['video_title']
            
            try:
                result = future.result()
                total_processed += 1
                
                if result['success']:
                    successful_summaries += 1
                    logging.info(f"Successfully summarized transcript for video {video_db_id}: {video_title}")
                else:
                    failed_summaries += 1
                    logging.error(f"Failed to summarize transcript for video {video_db_id}: {video_title} - {result['error']}")
                
                # Log progress
                progress_percent = (total_processed / len(transcripts_to_process)) * 100
                logging.info(f"Progress: {total_processed}/{len(transcripts_to_process)} ({progress_percent:.1f}%) | Success: {successful_summaries} | Failed: {failed_summaries}")
                
            except Exception as e:
                failed_summaries += 1
                total_processed += 1
                logging.error(f"Exception processing video {video_db_id}: {e}", exc_info=True)
    
    # Close the main thread's connection
    close_thread_connections()
    logging.info(f"Summarization complete. Processed {total_processed} transcripts. Success: {successful_summaries} | Failed: {failed_summaries}")

def main():
    """Main function to run the transcript summarization."""
    parser = argparse.ArgumentParser(description="Summarize video transcripts using Gemini API.")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Maximum number of concurrent workers. Default: {DEFAULT_MAX_WORKERS}"
    )
    parser.add_argument(
        "--api-key",
        help="Gemini API key (optional if GEMINI_API_KEY is in .env file)"
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=None,
        help="Maximum number of videos to process in this run"
    )
    args = parser.parse_args()
    
    logging.info(f"Starting Transcript Summarization at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get API key from command line or environment variable
    api_key = args.api_key or GEMINI_API_KEY
    if not api_key:
        logging.error("Gemini API key not provided via --api-key or GEMINI_API_KEY in .env file")
        sys.exit(1)
    
    # Process transcripts for summarization
    try:
        process_transcripts_for_summarization(
            max_workers=args.max_workers,
            api_key=api_key,
            max_videos=args.max_videos
        )
    except Exception as e:
        logging.critical(f"Critical error in main process: {e}", exc_info=True)
    
    logging.info(f"Transcript Summarization completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main() 