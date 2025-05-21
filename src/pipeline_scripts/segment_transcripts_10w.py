#!/usr/bin/env python3

import os
import argparse
import logging
import re
import sys
from datetime import datetime
from typing import Optional

# Assuming database_manager.py is in the same directory or accessible in PYTHONPATH
from database_manager import (
    create_connection,
    DATABASE_NAME,
    get_videos_for_10w_segmentation,
    update_video_segmentation_10w_status
)

# --- Configuration ---
LOG_FILE = "segment_transcripts_10w.log"
SCRIPT_NAME = "segment_transcripts_10w.py"
SEGMENT_LENGTH = 10 # Number of words per segment

# Default output directory for segmented transcripts
# It's good practice to make this configurable, perhaps relative to the original transcript dir
DEFAULT_SEGMENTED_TRANSCRIPTS_DIR = "E:\\transcripts_segmented_10w" # Using a different root for clarity

# --- Logging Setup ---
# Using a basic configuration for now, can be enhanced like in other scripts
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, 'a', 'utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(SCRIPT_NAME)

# --- Helper Functions ---

def ensure_dir_exists(directory_path: str):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logger.info(f"Created directory: {directory_path}")

def sanitize_filename(name: str) -> str:
    # Basic sanitization, can be expanded
    return "".join(c if c.isalnum() or c in ' ._-' else '_' for c in name).strip()

def parse_word_level_transcript_line(line: str):
    """
    Parses a line from the word-level transcript.
    Expected format: [start_time - end_time] word
    Returns a tuple (start_time, end_time, word) or None if parsing fails.
    """
    match = re.match(r'\[(\d+\.\d+) - (\d+\.\d+)\] (.+)', line)
    if match:
        try:
            start_time = float(match.group(1))
            end_time = float(match.group(2))
            word = match.group(3).strip()
            return start_time, end_time, word
        except ValueError:
            logger.warning(f"Could not parse time values in line: {line}")
            return None
    return None

def create_segmented_transcript(word_level_path: str, segmented_output_path: str, segment_length: int = SEGMENT_LENGTH):
    """
    Reads a word-level transcript, groups words into segments, and writes the segmented transcript.
    Returns True on success, False on failure.
    """
    if not os.path.exists(word_level_path):
        logger.error(f"Word-level transcript not found: {word_level_path}")
        return False

    try:
        with open(word_level_path, 'r', encoding='utf-8') as infile, \
             open(segmented_output_path, 'w', encoding='utf-8') as outfile:
            
            words_buffer = [] # Stores (start, end, word) tuples
            
            for line_number, line in enumerate(infile, 1):
                line = line.strip()
                if not line: # Skip empty lines
                    continue

                parsed_word = parse_word_level_transcript_line(line)
                if parsed_word:
                    words_buffer.append(parsed_word)
                    
                    if len(words_buffer) == segment_length:
                        # Write out the segment
                        segment_start_time = words_buffer[0][0]
                        segment_end_time = words_buffer[-1][1]
                        segment_text = " ".join([w[2] for w in words_buffer])
                        outfile.write(f"[{segment_start_time:.3f} - {segment_end_time:.3f}] {segment_text}\n\n")
                        words_buffer = [] # Clear buffer for next segment
                else:
                    logger.warning(f"Skipping unparsable line {line_number} in {word_level_path}: {line}")

            # Write any remaining words in the buffer as a final, possibly shorter, segment
            if words_buffer:
                segment_start_time = words_buffer[0][0]
                segment_end_time = words_buffer[-1][1]
                segment_text = " ".join([w[2] for w in words_buffer])
                outfile.write(f"[{segment_start_time:.3f} - {segment_end_time:.3f}] {segment_text}\n\n")
        
        logger.info(f"Successfully created segmented transcript: {segmented_output_path}")
        return True
    except IOError as e:
        logger.error(f"IOError processing transcript {word_level_path} to {segmented_output_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error processing transcript {word_level_path} to {segmented_output_path}: {e}")
        return False

# --- Main Execution ---
def main(output_base_dir: str, max_videos_to_process: Optional[int] = None):
    total_start_time = datetime.now()
    logger.info(f"--- Starting 10-Word Segmentation Script at {total_start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    logger.info(f"Segmented transcripts will be saved under: {output_base_dir}")
    if max_videos_to_process:
        logger.info(f"Processing a maximum of {max_videos_to_process} videos this run.")

    ensure_dir_exists(output_base_dir)
    
    conn = create_connection(DATABASE_NAME)
    if not conn:
        logger.error("Could not connect to the database. Exiting.")
        return

    videos_to_segment = get_videos_for_10w_segmentation(conn, limit=max_videos_to_process)
    if not videos_to_segment:
        logger.info("No videos found needing 10-word segmentation. Exiting.")
        if conn: conn.close()
        return

    processed_count = 0
    failed_count = 0

    for video_data in videos_to_segment:
        video_db_id = video_data['id']
        word_level_transcript_path = video_data['transcription_path']
        video_title = video_data['title'] # For logging/naming if needed

        logger.info(f"Processing video ID {video_db_id}: {video_title}")
        update_video_segmentation_10w_status(conn, video_db_id, 'segmenting', initiated=True)

        if not word_level_transcript_path or not os.path.exists(word_level_transcript_path):
            err_msg = f"Word-level transcript path missing or file not found: {word_level_transcript_path}"
            logger.error(err_msg)
            update_video_segmentation_10w_status(conn, video_db_id, 'failed', error_message=err_msg)
            failed_count += 1
            continue
        
        # Construct output path
        # Example: E:\transcripts_segmented_10w\OriginalName_transcript_10w.txt
        original_basename = os.path.basename(word_level_transcript_path)
        name_part, ext_part = os.path.splitext(original_basename)
        # Ensure it doesn't end with _transcript if it already does, to avoid _transcript_transcript_10w
        if name_part.endswith('_transcript'):
            name_part = name_part[:-len('_transcript')] 
            
        segmented_filename = f"{sanitize_filename(name_part)}_10w.txt" # Ensure this is just .txt, not .txt.txt
        segmented_output_full_path = os.path.join(output_base_dir, segmented_filename)

        if create_segmented_transcript(word_level_transcript_path, segmented_output_full_path):
            update_video_segmentation_10w_status(conn, video_db_id, 'completed', 
                                                segmented_transcript_path=segmented_output_full_path, 
                                                completed=True)
            processed_count += 1
        else:
            err_msg = f"Failed to create segmented transcript for {word_level_transcript_path}"
            logger.error(err_msg) # create_segmented_transcript logs specific error
            update_video_segmentation_10w_status(conn, video_db_id, 'failed', error_message=err_msg)
            failed_count += 1
            
    if conn:
        conn.close()
        logger.info("Database connection closed.")

    total_end_time = datetime.now()
    total_elapsed_time = total_end_time - total_start_time
    logger.info(f"--- 10-Word Segmentation Script Finished at {total_end_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    logger.info(f"Total runtime: {total_elapsed_time}")
    logger.info(f"Successfully segmented: {processed_count} videos.")
    logger.info(f"Failed to segment: {failed_count} videos.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Segments word-level transcripts into 10-word segments.")
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_SEGMENTED_TRANSCRIPTS_DIR,
        help=f"Base directory to save the segmented transcript files. Default: {DEFAULT_SEGMENTED_TRANSCRIPTS_DIR}"
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=None,
        help="Maximum number of videos to process in this run (optional)."
    )
    args = parser.parse_args()

    main(args.output_dir, args.max_videos) 