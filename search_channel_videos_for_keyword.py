#!/usr/bin/env python3

import os
import logging
import argparse
import sqlite3
import time
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

LOG_FILE = "search_channel_videos.log"
SCRIPT_NAME = "search_channel_videos.py"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ],
    encoding='utf-8'
)

# --- Helper Functions ---
def initialize_youtube_api():
    """Initializes and returns the YouTube Data API client."""
    try:
        youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=YOUTUBE_API_KEY)
        return youtube
    except Exception as e:
        logging.error(f"Failed to initialize YouTube API client: {e}")
        raise

def search_videos_by_title(youtube, channel_id, title_query, api_delay=0.5):
    """
    Searches for ALL videos from a specific channel with titles containing the specified query.
    Uses pagination to retrieve all matching videos.
    Returns a list of video details.
    """
    try:
        all_videos = []
        next_page_token = None
        
        # Keep fetching pages until no more results
        while True:
            # First, search for videos from the specified channel with titles containing the query
            search_params = {
                "part": "snippet",
                "channelId": channel_id,
                "q": title_query,
                "type": "video",
                "maxResults": 50  # Maximum allowed per page by the API
            }
            
            # Add page token if we're not on the first page
            if next_page_token:
                search_params["pageToken"] = next_page_token
                
            search_response = youtube.search().list(**search_params).execute()
            
            videos_in_page = []
            for item in search_response.get("items", []):
                video_id = item["id"]["videoId"]
                title = item["snippet"]["title"]
                description = item["snippet"]["description"]
                published_at = item["snippet"]["publishedAt"]
                channel_title = item["snippet"]["channelTitle"]
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                
                video_info = {
                    "video_id": video_id,
                    "title": title,
                    "description": description,
                    "published_at": published_at,
                    "channel_id": channel_id,
                    "channel_title": channel_title,
                    "video_url": video_url
                }
                videos_in_page.append(video_info)
            
            # Add the page's videos to our result list
            all_videos.extend(videos_in_page)
            logging.info(f"Found {len(videos_in_page)} videos on current page for channel {channel_id}")
            
            # Check if there are more pages
            next_page_token = search_response.get("nextPageToken")
            if not next_page_token:
                break  # No more pages, exit the loop
                
            # Small delay between pagination requests to respect API quotas
            time.sleep(api_delay)
            
        logging.info(f"Found {len(all_videos)} total videos in channel {channel_id} matching query '{title_query}'")
        return all_videos
        
    except HttpError as e:
        logging.error(f"HTTP error searching for videos in channel {channel_id}: {e}")
        if e.resp.status == 403:
            logging.error("Quota likely exceeded. Stopping.")
            raise  # Re-raise to stop the script
        return []
    except Exception as e:
        logging.error(f"Unexpected error searching for videos in channel {channel_id}: {e}")
        return []

def get_channel_details(youtube, channel_id):
    """Get channel details from the API."""
    try:
        channel_response = youtube.channels().list(
            part="snippet",
            id=channel_id
        ).execute()
        
        if not channel_response.get("items"):
            logging.error(f"No channel found with ID: {channel_id}")
            return None
            
        channel_info = channel_response["items"][0]["snippet"]
        return {
            "channel_id": channel_id,
            "channel_title": channel_info["title"],
            "channel_description": channel_info.get("description", "")
        }
    except HttpError as e:
        logging.error(f"HTTP error getting channel details for {channel_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error getting channel details for {channel_id}: {e}")
        return None

def add_video_to_db(conn, video_info, status="found"):
    """
    Adds a video to the database or updates an existing one based on video_id.
    """
    try:
        cursor = conn.cursor()
        
        # First, check if the video already exists
        cursor.execute("SELECT id FROM videos WHERE video_id = ?", (video_info["video_id"],))
        existing_video = cursor.fetchone()
        
        if existing_video:
            # Update existing video
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
            cursor.execute(
                sql, 
                (
                    video_info["title"],
                    video_info["channel_id"],
                    status,
                    SCRIPT_NAME,
                    video_info["published_at"],
                    video_info["video_url"],
                    video_info["video_id"]
                )
            )
            video_db_id = existing_video[0]
            action = "Updated"
        else:
            # Insert new video
            sql = '''
            INSERT INTO videos (
                video_id, 
                title, 
                channel_id, 
                status, 
                source_script,
                published_at,
                video_url,
                added_at, 
                last_updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            '''
            cursor.execute(
                sql, 
                (
                    video_info["video_id"],
                    video_info["title"],
                    video_info["channel_id"],
                    status,
                    SCRIPT_NAME,
                    video_info["published_at"],
                    video_info["video_url"]
                )
            )
            video_db_id = cursor.lastrowid
            action = "Added"
            
        # Also add a processing log
        cursor.execute('''
        INSERT INTO processing_logs 
        (video_record_id, stage, status, message, timestamp)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (video_db_id, "search", status, f"Video found via title search '{video_info.get('title_query', 'N/A')}'"))
        
        conn.commit()
        logging.info(f"{action} video: '{video_info['title']}' (ID: {video_info['video_id']}) in database")
        return video_db_id
        
    except sqlite3.Error as e:
        logging.error(f"Database error while recording video '{video_info['title']}' (ID: {video_info['video_id']}): {e}")
        return None

def ensure_channel_in_db(conn, channel_info):
    """
    Adds channel to the database if it doesn't exist.
    """
    try:
        cursor = conn.cursor()
        
        # Check if channel exists
        cursor.execute("SELECT id FROM channels WHERE channel_id = ?", (channel_info["channel_id"],))
        existing = cursor.fetchone()
        
        if existing:
            # Update channel
            sql = '''
            UPDATE channels SET 
                channel_title = ?, 
                source_script = ?,
                status = 'found',
                last_updated_at = CURRENT_TIMESTAMP 
            WHERE channel_id = ?
            '''
            cursor.execute(sql, (channel_info["channel_title"], SCRIPT_NAME, channel_info["channel_id"]))
            logging.info(f"Updated channel: {channel_info['channel_title']} (ID: {channel_info['channel_id']})")
        else:
            # Insert new channel
            sql = '''
            INSERT INTO channels (
                channel_id, 
                channel_title, 
                source_script, 
                status, 
                added_at, 
                last_updated_at
            ) VALUES (?, ?, ?, 'found', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            '''
            cursor.execute(sql, (channel_info["channel_id"], channel_info["channel_title"], SCRIPT_NAME))
            logging.info(f"Added new channel: {channel_info['channel_title']} (ID: {channel_info['channel_id']})")
            
        conn.commit()
        return True
    except sqlite3.Error as e:
        logging.error(f"Database error while recording channel {channel_info['channel_id']}: {e}")
        return False

def main(channel_ids, title_query, api_delay=0.5):
    """Main function to search for videos from specified channels with specific title patterns."""
    logging.info("--- Starting Channel Videos Search Script ---")
    logging.info(f"Searching for videos with title containing: '{title_query}'")
    logging.info(f"In {len(channel_ids)} channel(s): {', '.join(channel_ids)}")

    conn = create_connection(DATABASE_NAME)
    if not conn:
        logging.error("Could not connect to the database. Exiting.")
        return

    youtube = initialize_youtube_api()
    
    total_videos_found = 0
    total_videos_added = 0
    
    try:
        for channel_id in channel_ids:
            logging.info(f"Processing channel ID: {channel_id}")
            
            # Get channel details
            channel_info = get_channel_details(youtube, channel_id)
            if not channel_info:
                logging.error(f"Could not retrieve details for channel ID: {channel_id}. Skipping.")
                continue
                
            # Ensure channel is in database
            ensure_channel_in_db(conn, channel_info)
            
            # Search for videos (getting ALL matching videos)
            videos = search_videos_by_title(youtube, channel_id, title_query, api_delay)
            
            if not videos:
                logging.info(f"No videos found for channel {channel_id} with title containing '{title_query}'")
                continue
                
            logging.info(f"Found {len(videos)} videos for channel '{channel_info['channel_title']}' matching '{title_query}'")
            
            # Add videos to database
            for video in videos:
                video["title_query"] = title_query  # Add query for logging
                video_db_id = add_video_to_db(conn, video)
                if video_db_id:
                    total_videos_added += 1
                    
            total_videos_found += len(videos)
            
            # Respect API limits with small delay between calls
            if api_delay > 0 and len(channel_ids) > 1:
                time.sleep(api_delay)
                
    except HttpError as e:
        logging.critical(f"A critical YouTube API HttpError occurred (likely quota): {e}. Stopping script.")
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred: {e}. Stopping script.")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    logging.info("--- Channel Videos Search Script Finished ---")
    logging.info(f"Total videos found: {total_videos_found}")
    logging.info(f"Total videos added/updated in database: {total_videos_added}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search for videos from YouTube channels with specific title patterns.")
    parser.add_argument(
        "--channels",
        required=True,
        help="Comma-separated list of YouTube channel IDs to search"
    )
    parser.add_argument(
        "--title-query",
        required=True,
        help="String to search for in video titles"
    )
    parser.add_argument(
        "--api-delay",
        type=float,
        default=0.5,
        help="Delay in seconds between API calls for multiple channels and between pagination. Default: 0.5"
    )
    args = parser.parse_args()
    
    channel_ids = [c.strip() for c in args.channels.split(",")]
    
    main(channel_ids, args.title_query, args.api_delay) 