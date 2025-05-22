import argparse
import os
import sys
import json # Keep for other potential uses, or remove if truly no longer needed
from datetime import datetime
import sqlite3 # Added for type hinting and direct use if needed
import re # For extracting playlist ID
from pathlib import Path
import yt_dlp
from typing import Dict, Optional

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv() # Load variables from .env in the project root
except ImportError:
    print("Info: python-dotenv library not found. API key must be set as an environment variable or passed via --api-key argument.")
    # Continue without it, os.environ.get will just return None if not set elsewhere

# Attempt to import Google API client libraries
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    print("Error: The 'google-api-python-client' library is required to fetch playlist data from YouTube API.")
    print("Please install it by running: pip install google-api-python-client")
    sys.exit(1)

# Adjust the path to import from the parent directory's 'pipeline_scripts'
# This assumes create_custom_db.py is in src/pipeline_scripts/
# and database_manager.py is also in src/pipeline_scripts/
# For robust imports, especially if scripts are moved or run from different locations,
# consider making your project a package or adjusting PYTHONPATH.

# Get the absolute path to the directory containing the current script
current_script_dir = os.path.dirname(os.path.abspath(__file__))
# Get the absolute path to the project root (assuming project-barney is the root)
project_root = os.path.abspath(os.path.join(current_script_dir, "..", ".."))
# Get the absolute path to the src directory
src_dir = os.path.join(project_root, "src")

# Add src_dir to sys.path to allow imports from modules within src
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

try:
    from pipeline_scripts.database_manager import initialize_database, create_connection
except ImportError as e:
    print(f"Error importing database_manager: {e}")
    print("Please ensure database_manager.py is in the pipeline_scripts directory and the script is run from a location where it can be found.")
    print(f"Current sys.path: {sys.path}")
    sys.exit(1)

DEFAULT_DB_NAME = "test.db"
DATABASES_DIR_NAME = "databases"
SCRIPT_NAME = os.path.basename(__file__)
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def extract_playlist_id(playlist_url: str) -> str | None:
    """Extracts the YouTube playlist ID from a URL."""
    patterns = [
        r"(?:https?:\/\/)?(?:www\.)?youtube\.com\/playlist\?list=([a-zA-Z0-9_-]+)",
        r"(?:https?:\/\/)?(?:www\.)?youtube\.com\/embed\/videoseries\?list=([a-zA-Z0-9_-]+)",
        r"([a-zA-Z0-9_-]+)" # Assume if it's just an ID
    ]
    for pattern in patterns:
        match = re.search(pattern, playlist_url)
        if match:
            # Check if the matched string is likely a playlist ID (common length and chars)
            # Playlist IDs are typically 34 chars (e.g., PL...) or 13 chars (e.g., UU... for uploads)
            # This is a heuristic, YouTube might change ID formats.
            potential_id = match.group(1)
            if len(potential_id) > 10 and not potential_id.startswith("watch?v="): # Avoid capturing video IDs
                 # Further check: a common playlist ID format is PL... or UU... or OL...
                if potential_id.startswith(("PL", "UU", "FL", "OL", "RD")):
                    return potential_id
                # If it was the last pattern (generic), and it's a plausible length.
                if pattern == patterns[-1] and (len(potential_id) > 20 or potential_id.startswith("PL")): # Heuristic for bare IDs
                    return potential_id


    print(f"Warning: Could not extract a valid playlist ID from URL: {playlist_url}")
    return None

def get_playlist_videos_youtube_api(playlist_url: str, api_key: str) -> list:
    """
    Fetches video details from a YouTube playlist using the YouTube Data API v3.
    """
    playlist_id = extract_playlist_id(playlist_url)
    if not playlist_id:
        print(f"Could not extract playlist ID from URL: {playlist_url}")
        return []

    videos_metadata = []
    try:
        youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=api_key)

        # Step 1: Get all video IDs and initial snippet data from the playlist
        playlist_video_details = []
        next_page_token = None
        while True:
            pl_request = youtube.playlistItems().list(
                part="snippet,contentDetails", # contentDetails for videoId
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            pl_response = pl_request.execute()

            for item in pl_response.get("items", []):
                video_id = item.get("contentDetails", {}).get("videoId")
                if video_id: # Ensure videoId exists
                    playlist_video_details.append({
                        "video_id": video_id,
                        "title": item.get("snippet", {}).get("title", "N/A"),
                        "channel_id": item.get("snippet", {}).get("channelId", "N/A"),
                        # publishedAt here is when video was ADDED to playlist
                    })
            
            next_page_token = pl_response.get("nextPageToken")
            if not next_page_token:
                break
        
        if not playlist_video_details:
            print(f"No videos found in playlist ID: {playlist_id}")
            return []

        # Step 2: Get actual video publication dates for all collected video IDs
        all_video_ids = [vid_detail["video_id"] for vid_detail in playlist_video_details]
        video_specific_details = {} # Store details like actual publishedAt

        for i in range(0, len(all_video_ids), 50): # Process in batches of 50
            batch_ids = all_video_ids[i:i+50]
            vid_request = youtube.videos().list(
                part="snippet", # For snippet.publishedAt (original video publication)
                id=",".join(batch_ids)
            )
            vid_response = vid_request.execute()
            for item in vid_response.get("items", []):
                video_specific_details[item["id"]] = {
                    "published_at": item.get("snippet", {}).get("publishedAt")
                }
        
        # Step 3: Combine playlist item details with video specific details
        for p_item in playlist_video_details:
            vid_id = p_item["video_id"]
            actual_published_at = video_specific_details.get(vid_id, {}).get("published_at")
            
            if not actual_published_at:
                print(f"Warning: Could not fetch original published_at for video ID {vid_id}. Skipping this video.")
                continue

            videos_metadata.append({
                "video_id": vid_id,
                "title": p_item["title"],
                "video_url": f"https://www.youtube.com/watch?v={vid_id}",
                "channel_id": p_item["channel_id"],
                "published_at": actual_published_at # ISO 8601 format
            })

        print(f"Successfully fetched metadata for {len(videos_metadata)} videos from playlist ID {playlist_id} using YouTube API.")

    except HttpError as e:
        print(f"An HTTP error {e.resp.status} occurred: {e.content}")
        try:
            error_details = json.loads(e.content.decode())
            if 'error' in error_details and 'errors' in error_details['error']:
                for err in error_details['error']['errors']:
                    print(f"  Reason: {err.get('reason', 'N/A')}, Message: {err.get('message', 'N/A')}")
        except: # Fallback if content is not JSON or structure is unexpected
            pass
        return []
    except Exception as e:
        print(f"An unexpected error occurred while fetching playlist videos via API: {e}")
        return []
    
    return videos_metadata

def add_videos_to_db(conn: sqlite3.Connection, videos_metadata: list, source_script: str):
    """
    Adds video metadata to the videos table in the database.
    Expects 'published_at' in ISO 8601 format.
    """
    if not videos_metadata:
        print("No video metadata to add.")
        return

    cursor = conn.cursor()
    added_count = 0
    skipped_count = 0

    for video in videos_metadata:
        # 'published_at' is now expected to be an ISO 8601 string from the YouTube API
        # e.g., "2023-10-27T14:30:00Z"
        # SQLite can handle this format directly for TIMESTAMP columns.
        published_at_for_db = video.get("published_at")

        # Basic validation for essential fields
        if not all([video.get("video_id"), video.get("video_url"), video.get("title"), published_at_for_db]):
            print(f"Warning: Skipping video due to missing essential data: ID {video.get('video_id', 'N/A')}")
            continue
            
        try:
            cursor.execute("""
                INSERT INTO videos (video_id, video_url, channel_id, title, published_at, source_script, status)
                VALUES (?, ?, ?, ?, ?, ?, 'NEW')
                ON CONFLICT(video_id) DO NOTHING
            """, (video["video_id"], video["video_url"], video.get("channel_id"), video["title"], published_at_for_db, source_script))
            
            if cursor.rowcount > 0:
                added_count += 1
            else:
                skipped_count +=1
                
        except sqlite3.Error as e:
            print(f"Error inserting video {video['video_id']} into database: {e}")
            # Consider if you want to rollback or continue
    
    conn.commit()
    print(f"Finished adding videos to database. Added: {added_count}, Skipped (already existed or missing data): {skipped_count}")

def create_db_with_playlist(db_path: str, playlist_id_or_url: str, api_key: Optional[str] = None) -> Dict:
    """
    Creates a new SQLite database and populates it with videos from a YouTube playlist.
    Fetches date information directly from yt-dlp's entry data.
    """
    actual_playlist_id = playlist_id_or_url
    if "youtube.com" in playlist_id_or_url or "youtu.be" in playlist_id_or_url:
        match = re.search(r'list=([a-zA-Z0-9_-]+)', playlist_id_or_url)
        if match:
            actual_playlist_id = match.group(1)
        else:
            return {"error": "Could not extract playlist ID from URL"}

    try:
        db_dir = Path(db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(""" 
        CREATE TABLE IF NOT EXISTS videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT UNIQUE NOT NULL,
            video_url TEXT,
            title TEXT,
            channel_id TEXT,
            published_at TEXT, -- Storing as YYYY-MM-DD
            status TEXT DEFAULT 'NEW',
            subtitle_status TEXT DEFAULT 'pending_check',
            subtitle_file_path TEXT,
            subtitle_fetched_at TEXT,
            subtitle_error_message TEXT,
            text_source TEXT,
            arabic_plain_text_path TEXT,
            subtitle_to_text_status TEXT,
            subtitle_to_text_initiated_at TEXT,
            subtitle_to_text_completed_at TEXT,
            subtitle_to_text_error_message TEXT,
            download_status TEXT DEFAULT 'pending',
            transcription_status TEXT DEFAULT 'pending',
            segmentation_10w_status TEXT DEFAULT 'pending',
            analysis_status TEXT DEFAULT 'pending',
            ai_analysis_content TEXT,
            last_updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Configure yt-dlp: remove 'extract_flat': True to get more metadata per entry.
        # 'quiet', 'no_warnings', 'ignoreerrors' are still useful.
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'ignoreerrors': True, 
            'skip_download': True, # Ensure we don't download videos
            'extract_flat': False, # Set to False or remove to get more detailed entry info
            'forcejson': True, # Might help ensure consistent dictionary structure for entries
            'dump_single_json': False # We want to iterate entries if possible
        }

        videos_added = 0
        target_playlist_url = f'https://www.youtube.com/playlist?list={actual_playlist_id}'

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                print(f"Fetching playlist info for: {target_playlist_url} (this might take a moment for large playlists)...")
                playlist_info = ydl.extract_info(target_playlist_url, download=False)

                if not playlist_info or 'entries' not in playlist_info:
                    return {"error": f"Could not fetch playlist information or no entries found for {target_playlist_url}"}

                for entry in playlist_info.get('entries', []):
                    if not entry:  # Skip None entries (e.g., deleted videos in playlist)
                        continue

                    video_id = entry.get('id')
                    if not video_id:
                        print(f"Skipping entry with no video ID: {entry.get('title', 'N/A')}")
                        continue
                    
                    video_url = entry.get('webpage_url', f"https://www.youtube.com/watch?v={video_id}")
                    title = entry.get('title', 'Unknown Title')
                    channel_id = entry.get('channel_id') # Will be None if not found
                    published_at_date_str = None

                    # Try to get date: yt-dlp provides 'upload_date' as 'YYYYMMDD'
                    # or 'timestamp' as Unix epoch. We want 'YYYY-MM-DD'.
                    raw_timestamp = entry.get('timestamp')
                    upload_date_str_raw = entry.get('upload_date') # Format: YYYYMMDD
                    
                    if raw_timestamp is not None:
                        try:
                            dt_obj = datetime.fromtimestamp(raw_timestamp)
                            published_at_date_str = dt_obj.strftime('%Y-%m-%d')
                        except Exception as e:
                            print(f"[Video ID: {video_id}] Warning: Could not parse 'timestamp' {raw_timestamp}: {e}")
                    elif upload_date_str_raw and len(upload_date_str_raw) == 8:
                        try:
                            dt_obj = datetime.strptime(upload_date_str_raw, '%Y%m%d')
                            published_at_date_str = dt_obj.strftime('%Y-%m-%d')
                        except ValueError as e:
                            print(f"[Video ID: {video_id}] Warning: Could not parse 'upload_date' '{upload_date_str_raw}': {e}")
                    
                    if not published_at_date_str:
                        print(f"[Video ID: {video_id}] No valid date found (timestamp: {raw_timestamp}, upload_date: {upload_date_str_raw}). Storing as NULL.")
                    # else:
                        # print(f"DEBUG_YTDLP_DATE: Video ID {video_id}, Date: {published_at_date_str}") # Optional: re-enable if needed

                    try:
                        cursor.execute("""
                        INSERT INTO videos (
                            video_id, video_url, title, channel_id, published_at, status, subtitle_status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(video_id) DO UPDATE SET
                            video_url=excluded.video_url,
                            title=excluded.title,
                            channel_id=excluded.channel_id,
                            published_at=excluded.published_at,
                            status=CASE WHEN videos.status = 'NEW' THEN excluded.status ELSE videos.status END,
                            subtitle_status=CASE WHEN videos.subtitle_status = 'pending_check' THEN excluded.subtitle_status ELSE videos.subtitle_status END,
                            last_updated_at=CURRENT_TIMESTAMP
                        """, (
                            video_id, video_url, title, channel_id,
                            published_at_date_str, # This is now YYYY-MM-DD or None
                            'NEW', 'pending_check'
                        ))
                        if cursor.rowcount > 0:
                            videos_added += 1
                    except sqlite3.Error as e:
                        print(f"[Video ID: {video_id}] Error inserting into database: {e}")
                        continue

                conn.commit()
                return {"success": True, "videos_added": videos_added, "playlist_id": actual_playlist_id}

            except yt_dlp.utils.DownloadError as e:
                # Handle common yt-dlp errors like private/unavailable playlists
                print(f"yt-dlp DownloadError for playlist {target_playlist_url}: {e}")
                return {"error": f"yt-dlp could not process playlist {target_playlist_url}: {e.msg if hasattr(e, 'msg') else str(e)}"}
            except Exception as e:
                print(f"General error processing playlist {target_playlist_url} with yt-dlp: {e}")
                return {"error": f"Error processing playlist: {str(e)}"}

    except Exception as e:
        print(f"Database connection or setup failed for {db_path}: {e}")
        return {"error": f"Database creation failed: {str(e)}"}
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(
        description="Create/initialize a SQLite DB and optionally populate with YouTube playlist videos using YouTube Data API or yt-dlp."
    )
    parser.add_argument(
        "--db-name",
        default=DEFAULT_DB_NAME,
        help=f"Name of the SQLite database file to create/use. Default: {DEFAULT_DB_NAME}"
    )
    parser.add_argument(
        "--playlist-url", # Renamed from playlist_id for clarity with create_db_with_playlist
        type=str,
        default=None,
        help="URL or ID of the YouTube playlist to fetch video information from."
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=os.environ.get("YOUTUBE_API_KEY"),
        help="YouTube Data API v3 key. Optional; if not provided, only yt-dlp will be used if playlist_url is given."
    )
    args = parser.parse_args()

    db_filename = args.db_name
    if not db_filename.endswith(".db"):
        db_filename += ".db"
    
    databases_dir_path = Path(project_root) / DATABASES_DIR_NAME # Use Path object
    databases_dir_path.mkdir(parents=True, exist_ok=True)
    db_path = databases_dir_path / db_filename

    print(f"Attempting to initialize database schema at: {db_path}")
    # Initialize database function should create tables if they don't exist.
    # We call it here to ensure schema is up-to-date even if playlist processing fails.
    conn_init = sqlite3.connect(db_path)
    cursor_init = conn_init.cursor()
    # Re-paste your table creation SQL here from create_db_with_playlist or ensure initialize_database does it.
    cursor_init.execute(""" 
        CREATE TABLE IF NOT EXISTS videos ( 
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT UNIQUE NOT NULL, video_url TEXT, title TEXT, channel_id TEXT,
            published_at TEXT, status TEXT DEFAULT 'NEW', subtitle_status TEXT DEFAULT 'pending_check',
            subtitle_file_path TEXT, subtitle_fetched_at TEXT, subtitle_error_message TEXT,
            text_source TEXT, arabic_plain_text_path TEXT, subtitle_to_text_status TEXT,
            subtitle_to_text_initiated_at TEXT, subtitle_to_text_completed_at TEXT, subtitle_to_text_error_message TEXT,
            download_status TEXT DEFAULT 'pending', transcription_status TEXT DEFAULT 'pending',
            segmentation_10w_status TEXT DEFAULT 'pending', analysis_status TEXT DEFAULT 'pending',
            ai_analysis_content TEXT, last_updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn_init.commit()
    conn_init.close()
    print(f"Database schema for '{db_path}' ensured.")

    if args.playlist_url:
        print(f"Processing playlist: {args.playlist_url} using yt-dlp for basic info.")
        # Call create_db_with_playlist which now primarily uses yt-dlp
        result = create_db_with_playlist(str(db_path), args.playlist_url, args.api_key) # api_key is optional for this func now
        if result.get("success"):
            print(f"Successfully processed playlist. Videos added/updated: {result.get('videos_added', 0)}")
            print(f"FINAL_DB_PATH:{os.path.abspath(db_path)}") # Output path for Flet app to capture
        else:
            print(f"Failed to process playlist: {result.get('error', 'Unknown error')}")
            # If API key and YouTube API use was intended for more details, that logic could be here.
            # For now, main() relies on create_db_with_playlist's yt-dlp capabilities.
    else:
        print("No playlist URL provided. Only database schema initialization was performed.")
        print(f"FINAL_DB_PATH:{os.path.abspath(db_path)}") # Output path even if only initialized

    print(f"Script '{SCRIPT_NAME}' finished.")

if __name__ == '__main__':
    main() 