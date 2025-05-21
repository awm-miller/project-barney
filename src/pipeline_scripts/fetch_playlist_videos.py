#!/usr/bin/env python3

import os
import logging
import argparse
import sqlite3
import time
import re
from datetime import datetime, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# Import from our database manager
from database_manager import create_connection, DATABASE_NAME

# --- Configuration ---
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    raise ValueError("YOUTUBE_API_KEY not found in .env file or environment variables.")

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

LOG_FILE = "fetch_playlist_videos.log"
SCRIPT_NAME = "fetch_playlist_videos.py"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ],
    encoding='utf-8'
)
logger = logging.getLogger(SCRIPT_NAME)

# --- Helper Functions ---
def initialize_youtube_api():
    """Initializes and returns the YouTube Data API client."""
    try:
        youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=YOUTUBE_API_KEY)
        return youtube
    except Exception as e:
        logger.error(f"Failed to initialize YouTube API client: {e}")
        raise

def extract_playlist_id(playlist_url: str) -> str | None:
    """Extracts the playlist ID from a YouTube playlist URL."""
    patterns = [
        r"(?:https?:\/\/)?(?:www\.)?youtube\.com\/playlist\?list=([a-zA-Z0-9_-]+)",
        r"(?:https?:\/\/)?(?:www\.)?youtube\.com\/watch\?v=[a-zA-Z0-9_-]+&list=([a-zA-Z0-9_-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, playlist_url)
        if match:
            return match.group(1)
    logger.error(f"Could not extract playlist ID from URL: {playlist_url}")
    return None

def get_playlist_videos(youtube, playlist_id, api_delay=0.1):
    """
    Fetches all videos from a specific playlist, including their original publication dates.
    Uses pagination to retrieve all videos.
    Returns a list of video details.
    """
    try:
        all_videos = []
        next_page_token = None
        
        playlist_items_request_parts = "snippet,contentDetails" # contentDetails includes videoPublishedAt

        while True:
            playlist_items_params = {
                "part": playlist_items_request_parts,
                "playlistId": playlist_id,
                "maxResults": 50  # Maximum allowed per page
            }
            
            if next_page_token:
                playlist_items_params["pageToken"] = next_page_token
                
            playlist_items_response = youtube.playlistItems().list(**playlist_items_params).execute()
            
            videos_in_page = []
            for item in playlist_items_response.get("items", []):
                snippet = item.get("snippet", {})
                content_details = item.get("contentDetails", {})
                
                video_id = snippet.get("resourceId", {}).get("videoId")
                if not video_id:
                    logger.warning(f"Skipping item without videoId: {item}")
                    continue

                title = snippet.get("title", "N/A")
                # videoPublishedAt is the key for original video publication
                published_at_str = content_details.get("videoPublishedAt") 
                
                # Ensure published_at is in the correct format (YYYY-MM-DDTHH:MM:SSZ)
                if published_at_str:
                    try:
                        # Parse and reformat to ensure consistency if needed, though API usually gives ISO 8601
                        dt_obj = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
                        published_at_iso = dt_obj.isoformat()
                    except ValueError:
                        logger.warning(f"Could not parse videoPublishedAt: {published_at_str} for video {video_id}. Skipping date.")
                        published_at_iso = None
                else:
                    published_at_iso = None
                    logger.warning(f"videoPublishedAt not found for video {video_id}. Title: {title}")


                video_info = {
                    "video_id": video_id,
                    "title": title,
                    "description": snippet.get("description", ""),
                    "published_at": published_at_iso, # Actual video publication date
                    "channel_id": snippet.get("videoOwnerChannelId"),
                    "channel_title": snippet.get("videoOwnerChannelTitle"),
                    "video_url": f"https://www.youtube.com/watch?v={video_id}",
                    "playlist_id": playlist_id # For reference
                }
                videos_in_page.append(video_info)
            
            all_videos.extend(videos_in_page)
            logger.info(f"Fetched {len(videos_in_page)} videos from page for playlist {playlist_id}. Total fetched so far: {len(all_videos)}")
            
            next_page_token = playlist_items_response.get("nextPageToken")
            if not next_page_token:
                break 
                
            time.sleep(api_delay)
            
        logger.info(f"Fetched {len(all_videos)} total videos from playlist {playlist_id}")
        return all_videos
        
    except HttpError as e:
        logger.error(f"HTTP error fetching videos for playlist {playlist_id}: {e}")
        if e.resp.status == 403:
            logger.error("Quota likely exceeded. Stopping.")
            raise
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching videos for playlist {playlist_id}: {e}")
        return []

def add_video_to_db(conn, video_info, status="NEW"):
    """Adds a video to the database or updates an existing one based on video_id."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, status FROM videos WHERE video_id = ?", (video_info["video_id"],))
        existing_video = cursor.fetchone()
        
        current_time = datetime.now(timezone.utc).isoformat()

        if existing_video:
            video_db_id = existing_video[0]
            existing_status = existing_video[1]
            # Update existing video, potentially only if certain conditions are met
            # For now, let's update essential fields and keep status if it's already processed beyond 'NEW' or 'found'
            # or if new status is more "advanced" (e.g. don't override 'downloaded' with 'NEW')
            # This logic might need refinement based on exact pipeline needs.
            # A simple update for now:
            sql = '''
            UPDATE videos SET 
                title = ?, 
                channel_id = ?,
                status = ?, 
                source_script = ?,
                published_at = ?,
                video_url = ?,
                last_updated_at = CURRENT_TIMESTAMP 
            WHERE video_id = ?
            '''
            cursor.execute(sql, (video_info["title"], video_info["channel_id"], status, SCRIPT_NAME, video_info["published_at"], video_info["video_url"], video_info["video_id"]))
            action = "Updated"
        else:
            sql = '''
            INSERT INTO videos (
                video_id, title, channel_id, status, source_script, published_at, video_url, added_at, last_updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            '''
            cursor.execute(sql, (video_info["video_id"], video_info["title"], video_info["channel_id"], status, SCRIPT_NAME, video_info["published_at"], video_info["video_url"]))
            video_db_id = cursor.lastrowid
            action = "Added"
            
        # Add processing log
        cursor.execute('''
        INSERT INTO processing_logs (video_record_id, stage, status, message, source_script, timestamp)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (video_db_id, "discovery", status, f"Video found via playlist fetch (Playlist ID: {video_info.get('playlist_id', 'N/A')})", SCRIPT_NAME))
        
        conn.commit()
        logger.info(f"{action} video: '{video_info['title']}' (ID: {video_info['video_id']}, DB ID: {video_db_id}) with status '{status}' using published_at: {video_info['published_at']}")
        return video_db_id
        
    except sqlite3.Error as e:
        logger.error(f"Database error for video '{video_info['title']}' (ID: {video_info['video_id']}): {e}")
        conn.rollback()
        return None

def ensure_channel_in_db(conn, channel_id, channel_title):
    """Adds or updates channel in the database."""
    if not channel_id or not channel_title:
        logger.warning(f"Cannot ensure channel in DB without channel_id and channel_title. Skipping.")
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM channels WHERE channel_id = ?", (channel_id,))
        existing = cursor.fetchone()
        
        if existing:
            sql = '''UPDATE channels SET channel_title = ?, source_script = ?, status = 'found', last_updated_at = CURRENT_TIMESTAMP WHERE channel_id = ?'''
            cursor.execute(sql, (channel_title, SCRIPT_NAME, channel_id))
            logger.info(f"Updated channel: {channel_title} (ID: {channel_id})")
        else:
            sql = '''INSERT INTO channels (channel_id, channel_title, source_script, status, added_at, last_updated_at) VALUES (?, ?, ?, 'found', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)'''
            cursor.execute(sql, (channel_id, channel_title, SCRIPT_NAME))
            logger.info(f"Added new channel: {channel_title} (ID: {channel_id})")
            
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"Database error for channel {channel_id}: {e}")
        conn.rollback()
        return False

def main(playlist_url, date_after_str, api_delay):
    logger.info(f"--- Starting Playlist Video Fetch Script ---")
    logger.info(f"Fetching videos from playlist: {playlist_url}")
    logger.info(f"Filtering for videos published on or after: {date_after_str}")

    conn = create_connection(DATABASE_NAME)
    if not conn:
        logger.error("Could not connect to the database. Exiting.")
        return

    youtube = initialize_youtube_api()
    
    playlist_id = extract_playlist_id(playlist_url)
    if not playlist_id:
        logger.error("Failed to extract playlist ID. Exiting.")
        if conn: conn.close()
        return

    logger.info(f"Extracted Playlist ID: {playlist_id}")

    try:
        date_after_dt = datetime.strptime(date_after_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        logger.error(f"Invalid date format for --date-after. Please use YYYY-MM-DD. Exiting.")
        if conn: conn.close()
        return
        
    total_videos_processed = 0
    total_videos_added_or_updated = 0
    
    try:
        all_playlist_videos = get_playlist_videos(youtube, playlist_id, api_delay)
        
        if not all_playlist_videos:
            logger.info(f"No videos found in playlist {playlist_id}.")
        else:
            logger.info(f"Found {len(all_playlist_videos)} videos in total. Filtering by date...")

            filtered_videos = []
            for video in all_playlist_videos:
                if video.get("published_at"):
                    try:
                        video_published_dt = datetime.fromisoformat(video["published_at"].replace('Z', '+00:00'))
                        if video_published_dt >= date_after_dt:
                            filtered_videos.append(video)
                        else:
                            logger.debug(f"Video '{video['title']}' (Pub: {video_published_dt.strftime('%Y-%m-%d')}) is before target date {date_after_str}. Skipping.")
                    except ValueError as ve:
                        logger.warning(f"Could not parse published_at for video '{video['title']}' (ID: {video['video_id']}): {video['published_at']}. Error: {ve}. Skipping this video for date filter.")
                else:
                    logger.warning(f"Video '{video['title']}' (ID: {video['video_id']}) has no parsable publication date. Skipping for date filter.")
            
            logger.info(f"Found {len(filtered_videos)} videos published on or after {date_after_str}.")

            for video_info in filtered_videos:
                total_videos_processed += 1
                # Ensure channel exists in DB
                if video_info.get("channel_id") and video_info.get("channel_title"):
                    ensure_channel_in_db(conn, video_info["channel_id"], video_info["channel_title"])
                else:
                    logger.warning(f"Video '{video_info['title']}' (ID: {video_info['video_id']}) missing channel_id or channel_title. Cannot ensure channel in DB.")
                
                # Add video to DB
                video_db_id = add_video_to_db(conn, video_info, status="NEW") # Add as 'NEW' for pipeline
                if video_db_id:
                    total_videos_added_or_updated += 1
            
    except HttpError as e:
        logger.critical(f"A critical YouTube API HttpError occurred (likely quota): {e}. Stopping script.")
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred: {e}. Stopping script.")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

    logger.info("--- Playlist Video Fetch Script Finished ---")
    logger.info(f"Total videos processed from playlist (after date filter): {total_videos_processed}")
    logger.info(f"Total videos added/updated in database: {total_videos_added_or_updated}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch videos from a YouTube playlist, filter by date, and add to database.")
    parser.add_argument(
        "--playlist-url",
        required=True,
        help="Full URL of the YouTube playlist."
    )
    parser.add_argument(
        "--date-after",
        default="2023-09-01",
        help="Consider videos published on or after this date (YYYY-MM-DD). Default: 2023-09-01"
    )
    parser.add_argument(
        "--api-delay",
        type=float,
        default=0.1,
        help="Delay in seconds between API pagination calls. Default: 0.1"
    )
    args = parser.parse_args()
    
    main(args.playlist_url, args.date_after, args.api_delay) 