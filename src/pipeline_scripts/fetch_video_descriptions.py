#!/usr/bin/env python3

import os
import sqlite3
import logging
import argparse
import time
import subprocess
import json # For parsing yt-dlp output
import concurrent.futures # For rolling batch processing

# Import from our database manager
from database_manager import create_connection, DATABASE_NAME

# --- Configuration ---
# YOUTUBE_API_KEY is no longer needed if using yt-dlp
LOG_FILE = "fetch_video_descriptions.log"
SCRIPT_NAME = "fetch_video_descriptions.py" 

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(SCRIPT_NAME)

# Removed initialize_youtube_api function

def get_videos_needing_description(conn):
    """Fetches video_id and database id for videos where description is NULL or empty."""
    try:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id, video_id FROM videos WHERE description IS NULL OR description = '' ORDER BY id ASC")
        except sqlite3.OperationalError as e:
            if "no such column: description" in str(e):
                logger.error("The 'description' column does not exist in the 'videos' table. Please run database_manager.py to update schema.")
                return []
            raise
        
        videos = cursor.fetchall()
        logger.info(f"Found {len(videos)} videos needing descriptions.")
        return videos
    except sqlite3.Error as e:
        logger.error(f"Database error fetching videos needing descriptions: {e}")
        return []

def fetch_descriptions_with_ytdlp(video_ids_sub_batch):
    """Fetches descriptions for a sub-batch of video IDs using yt-dlp.
       This function is intended to be called by a worker thread.
    """
    video_details_map = {}
    if not video_ids_sub_batch:
        return video_details_map

    command = ['yt-dlp', '--skip-download', '-j'] + video_ids_sub_batch
    # logger.debug(f"[Worker] Executing yt-dlp for {len(video_ids_sub_batch)} videos: {' '.join(command)}")
    
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        stdout, stderr = process.communicate(timeout=300) # Added timeout (5 minutes per batch)

        if process.returncode != 0:
            logger.error(f"[Worker] yt-dlp error for batch (first ID: {video_ids_sub_batch[0]}). Exit code: {process.returncode}")
            logger.error(f"[Worker] yt-dlp stderr: {stderr.strip()}")
            return video_details_map 

        for line in stdout.strip().split('\n'):
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                video_id = data.get("id")
                description = data.get("description")
                if video_id:
                    video_details_map[video_id] = description if description is not None else ""
            except json.JSONDecodeError as e:
                logger.error(f"[Worker] Failed to parse JSON line from yt-dlp: {e}. Line: '{line[:200]}...'")
            except Exception as e:
                logger.error(f"[Worker] Error processing yt-dlp JSON object: {e}. Data: {line[:200]}...")
        
        # logger.debug(f"[Worker] Fetched details via yt-dlp for {len(video_details_map)} IDs in sub-batch.")
        return video_details_map
        
    except subprocess.TimeoutExpired:
        logger.error(f"[Worker] yt-dlp command timed out for batch starting with {video_ids_sub_batch[0]}.")
        if process: process.kill()
        return video_details_map
    except FileNotFoundError:
        logger.error("[Worker] yt-dlp command not found. Please ensure it is installed and in your PATH.")
        raise # Reraise to stop the main script if yt-dlp is not found
    except Exception as e:
        logger.error(f"[Worker] Unexpected error executing yt-dlp for batch starting with {video_ids_sub_batch[0]}: {e}", exc_info=True)
        return video_details_map


# Worker function for the ThreadPoolExecutor
def fetch_description_batch_worker(video_ids_sub_batch):
    """Worker to fetch descriptions for a sub-batch. Returns the map and any error."""
    try:
        descriptions_map = fetch_descriptions_with_ytdlp(video_ids_sub_batch)
        return {"descriptions_map": descriptions_map, "error": None, "batch_lead_id": video_ids_sub_batch[0] if video_ids_sub_batch else None}
    except Exception as e:
        # This will catch FileNotFoundError from fetch_descriptions_with_ytdlp if it raises
        logger.error(f"[Controller] Critical error in worker for batch starting with {video_ids_sub_batch[0] if video_ids_sub_batch else 'N/A'}: {e}", exc_info=True)
        return {"descriptions_map": {}, "error": str(e), "batch_lead_id": video_ids_sub_batch[0] if video_ids_sub_batch else None}


def update_video_descriptions_in_db(conn, video_description_map):
    """Updates the description for videos in the database for a given map.
       This function is called from the main thread after a worker completes.
    """
    updated_count = 0
    if not video_description_map:
        return 0
    try:
        cursor = conn.cursor()
        for video_id, description in video_description_map.items():
            try:
                sql = "UPDATE videos SET description = ?, last_updated_at = CURRENT_TIMESTAMP WHERE video_id = ?"
                cursor.execute(sql, (description, video_id))
                if cursor.rowcount > 0:
                    updated_count += 1
                    logger.debug(f"Updated description for video_id: {video_id}")
            except sqlite3.Error as e:
                logger.error(f"DB update error for video_id {video_id}: {e}")
        conn.commit()
        logger.info(f"DB: Committed updates for {updated_count} videos from a completed batch.")
        return updated_count
    except sqlite3.Error as e:
        logger.error(f"Database error during batch commit of descriptions: {e}")
        conn.rollback()
        return 0


def main(max_workers: int, limit_videos: int = None):
    ytdlp_batch_size = 1 # Hardcoded as per user request
    logger.info(f"--- Starting Fetch Video Descriptions Script (yt-dlp, rolling batch) ---")
    logger.info(f"Max concurrent yt-dlp processes: {max_workers}")
    logger.info(f"Video IDs per yt-dlp call (sub-batch size): {ytdlp_batch_size}")
    if limit_videos:
        logger.info(f"Processing at most {limit_videos} videos this run.")

    conn = create_connection(DATABASE_NAME)
    if not conn:
        logger.error("Could not connect to the database. Exiting.")
        return

    all_videos_needing_desc_tuples = get_videos_needing_description(conn)
    if not all_videos_needing_desc_tuples:
        logger.info("No videos require description fetching. Exiting.")
        if conn: conn.close(); return
    
    if limit_videos is not None and limit_videos < len(all_videos_needing_desc_tuples):
        logger.info(f"Limiting processing to {limit_videos} videos out of {len(all_videos_needing_desc_tuples)} needing descriptions.")
        all_videos_needing_desc_tuples = all_videos_needing_desc_tuples[:limit_videos]

    # Create sub-batches of video IDs for workers
    sub_batches_video_ids = []
    for i in range(0, len(all_videos_needing_desc_tuples), ytdlp_batch_size):
        # Each element in all_videos_needing_desc_tuples is (db_id, video_id)
        video_id_chunk = [tpl[1] for tpl in all_videos_needing_desc_tuples[i:i + ytdlp_batch_size]]
        if video_id_chunk:
            sub_batches_video_ids.append(video_id_chunk)

    if not sub_batches_video_ids:
        logger.info("No sub-batches to process. Exiting.")
        if conn: conn.close(); return

    total_sub_batches = len(sub_batches_video_ids)
    logger.info(f"Created {total_sub_batches} sub-batches for yt-dlp processing.")

    total_descriptions_fetched_api = 0
    total_descriptions_updated_db = 0
    sub_batches_processed_count = 0
    
    sub_batch_iterator = iter(sub_batches_video_ids)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            active_futures = set()

            while True:
                # Submit new tasks if there's capacity and sub-batches are available
                while len(active_futures) < max_workers:
                    try:
                        current_sub_batch = next(sub_batch_iterator)
                    except StopIteration:
                        current_sub_batch = None 
                        break 
                    
                    logger.info(f"[Controller] Submitting sub-batch starting with {current_sub_batch[0]} (size: {len(current_sub_batch)}) to executor.")
                    future = executor.submit(fetch_description_batch_worker, current_sub_batch)
                    active_futures.add(future)
                
                if not active_futures:
                    break # No more sub-batches to submit and no active tasks left

                done, active_futures_after_wait = concurrent.futures.wait(active_futures, return_when=concurrent.futures.FIRST_COMPLETED)
                active_futures = active_futures_after_wait

                for future in done:
                    sub_batches_processed_count += 1
                    try:
                        result = future.result() # This is the dict from fetch_description_batch_worker
                        batch_lead_id = result.get("batch_lead_id", "Unknown_Batch")
                        
                        if result.get("error"):
                            logger.error(f"[Controller] Worker for sub-batch {batch_lead_id} reported an error: {result['error']}")
                            if "yt-dlp command not found" in result.get("error", ""):
                                logger.critical("yt-dlp not found. Terminating script execution.")
                                # Cancel remaining futures (optional, or let them error out)
                                for f_cancel in active_futures: f_cancel.cancel()
                                raise FileNotFoundError("yt-dlp not found, stopping processing.")
                        else:
                            descriptions_map = result.get("descriptions_map", {})
                            if descriptions_map:
                                total_descriptions_fetched_api += len(descriptions_map)
                                updated_in_db = update_video_descriptions_in_db(conn, descriptions_map)
                                total_descriptions_updated_db += updated_in_db
                                logger.info(f"[Controller] Processed results for sub-batch {batch_lead_id}. Fetched: {len(descriptions_map)}, DB Updated: {updated_in_db}.")
                            else:
                                logger.warning(f"[Controller] Worker for sub-batch {batch_lead_id} returned no descriptions and no specific error.")

                    except Exception as exc:
                        # Error in future.result() or main thread processing logic
                        logger.error(f"[Controller] Critical error processing a completed future: {exc}", exc_info=True)
                    
                    logger.info(f"[Controller] Progress: {sub_batches_processed_count}/{total_sub_batches} sub-batches completed/processed.")
            
            logger.info("[Controller] All sub-batch processing attempts concluded.")

    except FileNotFoundError as fnf_error: # Specifically for yt-dlp not found from worker
        logger.critical(f"Halting due to critical error: {fnf_error}")
    except KeyboardInterrupt:
        logger.warning("--- Script interrupted by user (Ctrl+C). --- ")
    except Exception as e:
        logger.critical(f"--- An unexpected critical error occurred in the main processing loop: {e} ---", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

    logger.info(f"--- Fetch Video Descriptions Script (yt-dlp, rolling batch) Finished ---")
    logger.info(f"Total video descriptions successfully fetched (sum from batches): {total_descriptions_fetched_api}")
    logger.info(f"Total video descriptions updated in database: {total_descriptions_updated_db}")
    logger.info(f"Total sub-batches submitted to workers: {sub_batches_processed_count}/{total_sub_batches}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and update video descriptions from YouTube using yt-dlp with rolling batch processing.")
    parser.add_argument("--workers", type=int, default=2, help="Maximum number of concurrent yt-dlp batch processes. Default: 2")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of videos (not batches) to process in this run.")

    args = parser.parse_args()
    if args.workers <= 0: parser.error("Number of workers must be > 0")

    main(max_workers=args.workers, limit_videos=args.limit) 