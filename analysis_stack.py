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

# --- Load environment variables ---
load_dotenv()

# --- Configuration ---
LOG_FILE = "analysis_stack.log"

# Get required paths from environment variables
DATABASE_PATH = os.getenv("DATABASE_PATH")
if not DATABASE_PATH:
    raise ValueError("DATABASE_PATH not found in .env file. Please set it to your database file path.")

ANALYSIS_DIR = os.getenv("ANALYSIS_DIR")
if not ANALYSIS_DIR:
    raise ValueError("ANALYSIS_DIR not found in .env file. Please set it to your desired analysis output path.")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") # Already loaded, but good practice to check
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in .env file.")

# DEFAULT_DATABASE_PATH = r"E:\hate-preachers\test\downloads\transcription_data.db" # Removed hardcoded default
# DEFAULT_ANALYSIS_DIR = r"E:\hate-preachers\test\downloads\analysis" # Removed hardcoded default
DEFAULT_MAX_WORKERS = 4
DEFAULT_CHUNK_SIZE = 20  # Number of transcripts per API call

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

def get_pending_transcripts(conn: sqlite3.Connection, limit: int = None) -> List[Dict]:
    """Get videos with completed transcriptions but no AI analysis."""
    cursor = conn.cursor()
    
    query = '''
    SELECT id, video_title, channel_id, channel_title, video_url, video_path, transcript_path
    FROM video_processing 
    WHERE status = 'transcription_complete' AND (ai_analysis_path IS NULL OR ai_analysis_path = '')
    '''
    
    if limit:
        query += f' LIMIT {limit}'
    
    cursor.execute(query)
    
    results = []
    for row in cursor.fetchall():
        results.append({
            'id': row[0],
            'video_title': row[1],
            'channel_id': row[2],
            'channel_title': row[3],
            'video_url': row[4],
            'video_path': row[5],
            'transcript_path': row[6]
        })
    
    return results

def update_analysis_status(
    conn: sqlite3.Connection, 
    video_id: int, 
    status: str,
    ai_analysis_path: Optional[str] = None,
    ai_analysis_content: Optional[str] = None,
    error_message: Optional[str] = None
) -> None:
    """Update the analysis status for a video entry."""
    cursor = conn.cursor()
    
    update_fields = ["status = ?", "updated_at = CURRENT_TIMESTAMP"]
    params = [status]
    
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

def add_log_entry(
    conn: sqlite3.Connection,
    video_id: int,
    stage: str,
    status: str,
    message: str
) -> None:
    """Add a log entry to the processing_logs table."""
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO processing_logs 
    (video_id, stage, status, message)
    VALUES (?, ?, ?, ?)
    ''', (video_id, stage, status, message))
    
    conn.commit()

# --- Gemini API Functions ---
def initialize_gemini_api(api_key: str) -> None:
    """Initialize the Gemini API client."""
    genai.configure(api_key=api_key)
    logging.info("Initialized Gemini API client")

def analyze_transcript_chunk(
    transcripts: List[Dict],
    context: str,
    api_key: str,
    db_path: str,
    analysis_dir: str
) -> List[Dict]:
    """Analyze a chunk of transcripts using Gemini 2.5."""
    # Create a thread-local database connection
    conn = get_db_connection(db_path)
    
    # Ensure analysis directory exists
    os.makedirs(analysis_dir, exist_ok=True)
    
    # Initialize Gemini API
    initialize_gemini_api(api_key)
    
    try:
        # Build prompt with context and transcripts
        prompt = f"{context}\n\n"
        
        # Add information about each transcript in the chunk
        transcript_data = []
        for i, transcript_info in enumerate(transcripts):
            video_id = transcript_info['id']
            video_title = transcript_info['video_title']
            channel_title = transcript_info['channel_title']
            transcript_path = transcript_info['transcript_path']
            
            # Check if transcript file exists
            if not os.path.exists(transcript_path):
                error_msg = f"Transcript file not found: {transcript_path}"
                logging.error(error_msg)
                update_analysis_status(
                    conn, 
                    video_id, 
                    "analysis_failed", 
                    error_message=error_msg
                )
                add_log_entry(
                    conn,
                    video_id,
                    stage="analysis",
                    status="error",
                    message=error_msg
                )
                continue
            
            # Read transcript content
            try:
                with open(transcript_path, 'r', encoding='utf-8') as f:
                    transcript_content = f.read()
                
                # Add to transcript data
                transcript_data.append({
                    "id": video_id,
                    "index": i,
                    "video_title": video_title,
                    "channel_title": channel_title,
                    "transcript": transcript_content
                })
                
                # Log pending analysis
                update_analysis_status(conn, video_id, "analysis_pending")
                add_log_entry(
                    conn,
                    video_id,
                    stage="analysis",
                    status="pending",
                    message=f"Added to analysis chunk"
                )
                
            except Exception as e:
                error_msg = f"Error reading transcript file {transcript_path}: {str(e)}"
                logging.error(error_msg)
                update_analysis_status(
                    conn, 
                    video_id, 
                    "analysis_failed", 
                    error_message=error_msg
                )
                add_log_entry(
                    conn,
                    video_id,
                    stage="analysis",
                    status="error",
                    message=error_msg
                )
        
        # If no valid transcripts, return empty list
        if not transcript_data:
            logging.warning("No valid transcripts in chunk")
            return []
        
        # Convert transcript data to JSON string for the prompt
        transcript_json = json.dumps(transcript_data, indent=2)
        
        # Complete the prompt with instructions
        prompt += f"""
You are an expert on Islamist extremism given {len(transcript_data)} sermon transcripts in this JSON structure:
{transcript_json}

TASK
For *each* sermon, locate every passage that shows ANY of the following:
• extremist ideology or propaganda  
• hate speech (attacks on a protected group, slurs, dehumanisation)  
• direct or implied calls/incitement to violence  
• other clearly controversial or concerning rhetoric

When you find a passage that meets one of those criteria, extract:
  – timestamp (as given in the transcript) - ensure an accurate timestamp is given  
  – the *verbatim* quotation (no paraphrase or [...])  
  – category   one of ["extremist ideology", "hate speech", "incitement to violence", "other concerning"]  
  – brief_reason (≤ 20 words) explaining why it was flagged

RULES
• Ground every judgment in the quoted words alone. No speculation.  
• Do not add commentary.  
• Include *all* qualifying passages, even if the same theme repeats.  
• Omit everything that does *not* meet the criteria.  
• Do not change field names or output extra text.
• NEVER EVER make anything up. NEVER EVER invent quotes.

If there is no content that meets the criteria, return an empty array, but keep a summary.

OUTPUT
Return a single valid JSON array, one object per sermon, preserving the input order:

[
  {{
    "id": "<original id>",
    "video_title": "<original title>",
    "flags": [
      {{
        "timestamp": "00:12:34.560",
        "quote": "We must wipe them from the face of the earth.",
        "category": "incitement to violence",
        "brief_reason": "explicit call to eradicate a group"
      }},
      ...
    ],
    "summary": "Contains repeated calls for violence against unnamed enemies."
  }},
  ...
]

• The **flags** list may be empty if nothing meets the threshold.  
• The top-level array is the ONLY output—no prose, no markdown.
"""
        
        # Create Gemini model client
        model = genai.GenerativeModel('gemini-2.5-flash-preview-04-17')
        
        # Generate analysis
        try:
            response = model.generate_content(prompt)
            analysis_result = response.text
            
            # Parse JSON response
            try:
                # Clean up response if it contains markdown formatting
                if analysis_result.startswith("```"):
                    # Remove markdown code blocks
                    analysis_result = analysis_result.strip()
                    if analysis_result.startswith("```json"):
                        analysis_result = analysis_result[7:]  # Remove ```json
                    elif analysis_result.startswith("```"):
                        analysis_result = analysis_result[3:]  # Remove ```
                    
                    if analysis_result.endswith("```"):
                        analysis_result = analysis_result[:-3]  # Remove trailing ```
                
                analysis_json = json.loads(analysis_result)
                
                # Process each analyzed transcript
                results = []
                for analysis in analysis_json:
                    video_id = analysis.get('id')
                    video_title = analysis.get('video_title')
                    
                    # Create output file
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    # Sanitize title for filename - remove all invalid Windows filename characters
                    safe_title = video_title.replace(' ', '_')
                    safe_title = ''.join(c for c in safe_title if c not in '<>:"/\\|?*')
                    safe_title = safe_title[:50]  # Truncate to reasonable length
                    analysis_filename = f"analysis_{video_id}_{safe_title}_{timestamp}.json"
                    analysis_path = os.path.join(analysis_dir, analysis_filename)
                    
                    # Save analysis to file
                    with open(analysis_path, 'w', encoding='utf-8') as f:
                        json.dump(analysis, f, indent=2)
                    
                    # Update database
                    update_analysis_status(
                        conn, 
                        video_id, 
                        "analysis_complete", 
                        ai_analysis_path=analysis_path,
                        ai_analysis_content=json.dumps(analysis)
                    )
                    
                    add_log_entry(
                        conn,
                        video_id,
                        stage="analysis",
                        status="success",
                        message=f"Analysis saved to {analysis_path}"
                    )
                    
                    # Add to results
                    results.append({
                        'id': video_id,
                        'video_title': video_title,
                        'analysis_path': analysis_path
                    })
                
                return results
                
            except json.JSONDecodeError as e:
                error_msg = f"Failed to parse Gemini API response as JSON: {str(e)}"
                logging.error(error_msg)
                logging.error(f"Raw response: {analysis_result}")
                
                # Mark all transcripts in chunk as failed
                for transcript_info in transcript_data:
                    video_id = transcript_info['id']
                    update_analysis_status(
                        conn, 
                        video_id, 
                        "analysis_failed", 
                        error_message=error_msg
                    )
                    add_log_entry(
                        conn,
                        video_id,
                        stage="analysis",
                        status="error",
                        message=error_msg
                    )
                
                return []
                
        except GoogleAPIError as e:
            error_msg = f"Gemini API error: {str(e)}"
            logging.error(error_msg)
            
            # Mark all transcripts in chunk as failed
            for transcript_info in transcript_data:
                video_id = transcript_info['id']
                update_analysis_status(
                    conn, 
                    video_id, 
                    "analysis_failed", 
                    error_message=error_msg
                )
                add_log_entry(
                    conn,
                    video_id,
                    stage="analysis",
                    status="error",
                    message=error_msg
                )
            
            return []
            
    except Exception as e:
        error_msg = f"Unexpected error during analysis: {str(e)}"
        logging.error(error_msg)
        
        # Mark all transcripts in chunk as failed if possible
        if 'transcript_data' in locals():
            for transcript_info in transcript_data:
                video_id = transcript_info['id']
                update_analysis_status(
                    conn, 
                    video_id, 
                    "analysis_failed", 
                    error_message=error_msg
                )
                add_log_entry(
                    conn,
                    video_id,
                    stage="analysis",
                    status="error",
                    message=error_msg
                )
        
        return []
    finally:
        # Close the thread-local connection
        conn.close()
        if hasattr(thread_local, "connections"):
            delattr(thread_local, "connections")

def process_transcript_chunks(
    conn: sqlite3.Connection,
    db_path: str,
    analysis_dir: str,
    api_key: str,
    max_workers: int = DEFAULT_MAX_WORKERS,
    chunk_size: int = DEFAULT_CHUNK_SIZE
) -> None:
    """Process transcripts in chunks using parallel workers."""
    # Get all pending transcripts
    pending_transcripts = get_pending_transcripts(conn)
    logging.info(f"Found {len(pending_transcripts)} transcripts pending analysis")
    
    if not pending_transcripts:
        logging.info("No transcripts to analyze")
        return
    
    # Split transcripts into chunks
    transcript_chunks = [
        pending_transcripts[i:i + chunk_size] 
        for i in range(0, len(pending_transcripts), chunk_size)
    ]
    
    logging.info(f"Split transcripts into {len(transcript_chunks)} chunks of up to {chunk_size} transcripts each")
    
    # Prepare context about what we're analyzing
    context = """
    You are an expert in religious discourse analysis, particularly of sermons. 
    You're examining transcripts from religious sermons to identify potentially concerning content 
    including extremist ideology, hate speech, and calls to violence.
    Your goal is to provide an objective analysis that flags concerning rhetoric while remaining neutral and factual.
    """
    
    # Create thread pool for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit analysis jobs
        future_to_chunk = {
            executor.submit(
                analyze_transcript_chunk, 
                chunk, 
                context, 
                api_key, 
                db_path, 
                analysis_dir
            ): i for i, chunk in enumerate(transcript_chunks)
        }
        
        # Process results as they complete
        completed_chunks = 0
        for future in concurrent.futures.as_completed(future_to_chunk):
            chunk_index = future_to_chunk[future]
            try:
                results = future.result()
                completed_chunks += 1
                logging.info(f"Completed chunk {chunk_index+1}/{len(transcript_chunks)} with {len(results)} analyzed transcripts")
                logging.info(f"Progress: {completed_chunks}/{len(transcript_chunks)} chunks complete")
            except Exception as e:
                logging.error(f"Exception processing chunk {chunk_index+1}: {e}")
    
    logging.info(f"Analysis complete for all chunks")

def main():
    """Main function to run the analysis stack."""
    parser = argparse.ArgumentParser(description="Analyze video transcripts using Gemini API.")
    parser.add_argument(
        "--db-path",
        default=DATABASE_PATH, # Default comes from env var
        help="Path to the SQLite database file. Overrides DATABASE_PATH environment variable."
    )
    parser.add_argument(
        "--analysis-dir",
        default=ANALYSIS_DIR, # Default comes from env var
        help="Directory to save analysis JSON files. Overrides ANALYSIS_DIR environment variable."
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Maximum number of concurrent workers"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Number of transcripts per API call"
    )
    parser.add_argument(
        "--api-key",
        help="Gemini API key (optional if GEMINI_API_KEY is in .env file)"
    )
    args = parser.parse_args()
    
    logging.info("Starting Analysis Stack")
    
    # Get API key from command line or environment variable
    api_key = args.api_key or GEMINI_API_KEY
    if not api_key:
        logging.error("Gemini API key not provided via --api-key or GEMINI_API_KEY in .env file")
        sys.exit(1)
    
    # Initialize database connection
    conn = get_db_connection(args.db_path)
    
    try:
        # Ensure analysis directory exists
        os.makedirs(args.analysis_dir, exist_ok=True)
        
        # Process transcripts in chunks
        process_transcript_chunks(
            conn,
            args.db_path,
            args.analysis_dir,
            api_key,
            args.max_workers,
            args.chunk_size
        )
        
    finally:
        # Close the main thread's database connection
        conn.close()
        logging.debug("Closed main thread's database connection")
        
        # Cleanup thread-local connections
        try:
            close_thread_connections()
            logging.debug("Cleaned up thread-local connections")
        except Exception as e:
            logging.warning(f"Error during thread connection cleanup: {e}")
    
    logging.info("Analysis Stack completed")

if __name__ == "__main__":
    main() 