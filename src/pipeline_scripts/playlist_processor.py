import sqlite3
import subprocess
import os
import json
from datetime import datetime
import re

# Assuming database_manager.py and convert_subtitle_to_text.py are in the same directory or accessible
# If not, adjust import paths or ensure they are installed as part of a package.
# from .database_manager import create_connection, add_video_to_db # Example if in a package
# from .convert_subtitle_to_text import convert_srt_to_plain_text # Example

# For standalone script use, we might need to copy or adapt relevant functions if direct import is an issue.
# For now, let's assume we can define necessary DB interaction and SRT conversion logic here or import.

# --- Copied/Adapted from convert_subtitle_to_text.py ---
def convert_srt_to_plain_text(srt_file_path):
    """Parses an SRT file and returns its content as plain text, preserving timestamps."""
    if not os.path.exists(srt_file_path):
        return None, "SRT file not found"
    
    processed_lines = []
    try:
        with open(srt_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        current_entry_lines = []
        for line_num, line_content in enumerate(lines):
            stripped_line = line_content.strip()
            
            if stripped_line.isdigit():
                # This is likely a sequence number. If it's followed by a timestamp, or if current_entry_lines is empty,
                # it's safe to assume it's a sequence number for a new entry.
                # If there's content in current_entry_lines, it means the previous entry ended.
                if current_entry_lines:
                    processed_lines.extend(current_entry_lines)
                    processed_lines.append("") # Add a blank line between entries
                    current_entry_lines = []
                # Regardless, we skip the sequence number itself.
                continue 
            elif '-->' in stripped_line:
                # This is a timestamp line. If current_entry_lines has content, it means a malformed SRT
                # (e.g. text before timestamp without sequence number), or we are starting a new entry.
                # For simplicity, we'll assume it's the start of a new entry's timestamp.
                if current_entry_lines: # Clear previous if any (e.g. if no seq number started it)
                    processed_lines.extend(current_entry_lines)
                    processed_lines.append("")
                    current_entry_lines = []
                current_entry_lines.append(stripped_line)
            elif stripped_line: # Non-empty line, assumed to be text
                current_entry_lines.append(stripped_line)
            elif not stripped_line and current_entry_lines: 
                # An empty line, but we have content in current_entry.
                # This means an entry is finished.
                processed_lines.extend(current_entry_lines)
                processed_lines.append("") # Add a blank line for separation
                current_entry_lines = []
            # If it's a blank line and current_entry_lines is also empty, just ignore it (multiple blank lines)

        # Add any remaining content from the last entry
        if current_entry_lines:
            processed_lines.extend(current_entry_lines)

        # Clean up trailing empty lines if any were added excessively
        while processed_lines and not processed_lines[-1].strip():
            processed_lines.pop()
                
        if not processed_lines and lines: # File had content but no text extracted
             return None, "SRT file seems to contain no valid subtitle text entries after processing."

        return "\n".join(processed_lines), None
        
    except Exception as e:
        return None, f"Error parsing SRT file {os.path.basename(srt_file_path)}: {e}"
# --- End Copied/Adapted ---


# --- Database interaction (simplified for this module) ---
def create_db_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        # Consider re-raising or handling more robustly for the Flet app
    return conn

def add_video_to_database(conn, video_data):
    """
    Adds a video and its initial processing status to the database.
    video_data is a dictionary with keys like:
    video_id, video_url, channel_id, title, published_at,
    subtitle_file_path_en, subtitle_file_path_ar,
    plain_text_subtitle_path_en, plain_text_subtitle_path_ar
    """
    sql = """INSERT INTO videos (
        video_id, video_url, channel_id, title, published_at,
        subtitle_status, 
        subtitle_file_path, 
        plain_text_subtitle_path, 
        subtitle_fetched_at,
        subtitle_to_text_status,
        subtitle_to_text_completed_at,
        source_script,
        status 
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'NEW')
    ON CONFLICT(video_id) DO UPDATE SET
        video_url = excluded.video_url,
        channel_id = excluded.channel_id,
        title = excluded.title,
        published_at = excluded.published_at,
        subtitle_status = excluded.subtitle_status,
        subtitle_file_path = excluded.subtitle_file_path,
        plain_text_subtitle_path = excluded.plain_text_subtitle_path,
        subtitle_fetched_at = excluded.subtitle_fetched_at,
        subtitle_to_text_status = excluded.subtitle_to_text_status,
        subtitle_to_text_completed_at = excluded.subtitle_to_text_completed_at,
        source_script = excluded.source_script,
        last_updated_at = CURRENT_TIMESTAMP""" # Ensure no backslash after opening or before closing """
    
    cursor = conn.cursor()
    try:
        # For now, let's prioritize English subtitles if available, or decide a strategy
        # This schema might need adjustment if we store paths for multiple languages separately.
        # For simplicity, using the English path if available.
        sub_file_path_to_store = video_data.get('subtitle_file_path_en') or video_data.get('subtitle_file_path_ar')
        plain_text_path_to_store = video_data.get('plain_text_subtitle_path_en') or video_data.get('plain_text_subtitle_path_ar')
        
        sub_status = 'fetched' if sub_file_path_to_store else 'unavailable'
        sub_fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if sub_file_path_to_store else None
        
        text_conv_status = 'completed' if plain_text_path_to_store else 'pending' # or failed if sub was fetched but conversion failed
        text_conv_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if plain_text_path_to_store else None

        cursor.execute(sql, (
            video_data['video_id'],
            video_data['video_url'],
            video_data.get('channel_id'),
            video_data.get('title'),
            video_data.get('published_at'),
            sub_status,
            sub_file_path_to_store,
            plain_text_path_to_store,
            sub_fetched_at,
            text_conv_status,
            text_conv_at,
            'playlist_processor.py'
        ))
        conn.commit()
        return cursor.lastrowid
    except sqlite3.Error as e:
        print(f"Database error adding video {video_data.get('video_id', 'N/A')}: {e}") # Added fallback for video_id
        conn.rollback()
        return None
    except KeyError as e:
        print(f"KeyError in video_data for add_video_to_database: {e}. Data: {video_data}")
        conn.rollback()
        return None
# --- End Database interaction ---


def fetch_video_details_and_subtitles(video_id_or_url, base_subtitle_dir, progress_callback=None):
    """
    Fetches video metadata and subtitles for a single video using yt-dlp.
    Downloads English (en) and Arabic (ar) subtitles if available, converts them to SRT,
    then converts SRT to plain text.

    Args:
        video_id_or_url (str): The YouTube video ID or full URL.
        base_subtitle_dir (str): The root directory where 'raw_subtitles' and 'text_subtitles' will be created.
        progress_callback (function, optional): Callback for progress updates.
                                                Receives (current_task_description, percentage).

    Returns:
        dict: Video details including paths to subtitle files, or None if error.
              Keys: video_id, title, upload_date, channel_id, channel_url, description,
                    original_url, subtitle_file_path_en, subtitle_file_path_ar,
                    plain_text_subtitle_path_en, plain_text_subtitle_path_ar
    """
    video_url = f"https://www.youtube.com/watch?v={video_id_or_url}" if not video_id_or_url.startswith("http") else video_id_or_url
    
    raw_subs_dir = os.path.join(base_subtitle_dir, "raw_subtitles")
    text_subs_dir = os.path.join(base_subtitle_dir, "text_subtitles")
    os.makedirs(raw_subs_dir, exist_ok=True)
    os.makedirs(text_subs_dir, exist_ok=True)

    video_info = {}
    
    # Step 1: Get video metadata (title, upload_date, channel_id, etc.)
    if progress_callback:
        progress_callback("Fetching video metadata...", 0)
    
    # Using --dump-json to get metadata.
    # It's often more reliable to get metadata separately then download subs.
    # yt-dlp can be tricky with output templates when also downloading subs.
    cmd_info = [
        'yt-dlp',
        '--skip-download', # We only want metadata
        '--dump-json',     # Output metadata as JSON
        video_url
    ]
    
    try:
        process_info = subprocess.Popen(cmd_info, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        stdout_info, stderr_info = process_info.communicate(timeout=60)

        if process_info.returncode == 0 and stdout_info:
            data = json.loads(stdout_info)
            video_info['video_id'] = data.get('id')
            video_info['title'] = data.get('title')
            # yt-dlp's upload_date is YYYYMMDD, convert to ISO 8601 for consistency
            upload_date_str = data.get('upload_date')
            if upload_date_str:
                video_info['upload_date'] = datetime.strptime(upload_date_str, '%Y%m%d').strftime('%Y-%m-%dT%H:%M:%S')
            else:
                video_info['upload_date'] = None # Or use current time, or handle as error
            video_info['channel_id'] = data.get('channel_id')
            video_info['channel_url'] = data.get('channel_url')
            video_info['description'] = data.get('description')
            video_info['original_url'] = data.get('webpage_url', video_url) # Prefer webpage_url if available
            if not video_info['video_id']: # Should not happen if dump-json worked
                 raise ValueError("Failed to extract video ID from yt-dlp JSON output.")
            video_id = video_info['video_id'] # Use the definitive ID from metadata
        else:
            print(f"Error fetching video metadata for {video_url}: {stderr_info}")
            if progress_callback: progress_callback(f"Error metadata: {stderr_info}", 0) # No real percentage here
            return None
    except subprocess.TimeoutExpired:
        print(f"Timeout fetching video metadata for {video_url}")
        if progress_callback: progress_callback("Timeout fetching metadata", 0)
        return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON metadata for {video_url}. Output: {stdout_info}")
        if progress_callback: progress_callback("Error decoding metadata JSON", 0)
        return None
    except Exception as e:
        print(f"An exception occurred fetching video metadata for {video_url}: {e}")
        if progress_callback: progress_callback(f"Exception during metadata: {e}", 0)
        return None

    if progress_callback:
        progress_callback(f"Metadata OK for {video_info.get('title', video_id)}", 25) # Arbitrary progress

    # Step 2: Download subtitles
    # Output template for subtitles needs to be specific to avoid overwriting if multiple videos share an ID (not typical for YouTube)
    # and to handle multiple languages.
    # yt-dlp names files like: VideoTitle-VideoID.lang.ext or VideoID.lang.ext
    # We need a stable way to find these files. Using video_id in the filename is best.
    
    # Subtitle language preference: English, then Arabic
    langs_to_try = ['en', 'ar']
    downloaded_srt_files = {} # lang_code: path

    for lang_code in langs_to_try:
        if progress_callback:
            progress_callback(f"Fetching {lang_code} subs for {video_info.get('title', video_id)}...", 30 + (langs_to_try.index(lang_code) * 20) )

        # Define output path for this specific language's SRT
        # yt-dlp will add '.srt' if conversion is successful
        # Using a simple template, yt-dlp handles the .lang.srt part
        # The output template should not include the extension if --convert-subs is used with a specific format
        # However, to be safe, we specify the full desired path.
        # yt-dlp is a bit finicky with -o and subtitle naming.
        # A safer approach is to let yt-dlp name it with --output "%(id)s.%(lang)s.%(ext)s"
        # inside the raw_subs_dir, then find it.

        # Let yt-dlp name the file in raw_subs_dir
        output_template_sub = os.path.join(raw_subs_dir, f"{video_id}.%(ext)s") # Let yt-dlp determine extension, we'll filter for .srt later
        # If we want to be more specific about lang:
        # output_template_sub = os.path.join(raw_subs_dir, f"{video_id}.{lang_code}.%(ext)s") # This might be better

        # Simpler: let yt-dlp create files like VideoID.en.srt in raw_subs_dir
        # We set CWD for yt-dlp to raw_subs_dir to simplify output paths.

        cmd_subs = [
            'yt-dlp',
            '--write-subs',
            '--write-auto-subs', # Get auto-generated if manual not available
            '--sub-langs', lang_code,
            '--convert-subs', 'srt',
            '--skip-download',    # Don't download the video itself
            '--no-overwrites',    # Don't overwrite if file exists (e.g. from previous run)
            '--output', f"{video_id}.{lang_code}.%(ext)s", # Filename pattern, yt-dlp ensures .srt if converted
            video_url
        ]
        
        # print(f"Subtitle command: {' '.join(cmd_subs)} in dir {raw_subs_dir}")
        try:
            # Run from raw_subs_dir to ensure output files are predictably named and located
            process_subs = subprocess.Popen(cmd_subs, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', cwd=raw_subs_dir)
            stdout_subs, stderr_subs = process_subs.communicate(timeout=180) # 3 min timeout for subs

            expected_srt_filename = f"{video_id}.{lang_code}.srt"
            expected_srt_path = os.path.join(raw_subs_dir, expected_srt_filename)

            if os.path.exists(expected_srt_path):
                downloaded_srt_files[lang_code] = expected_srt_path
                video_info[f'subtitle_file_path_{lang_code}'] = expected_srt_path
                print(f"Successfully downloaded {lang_code} subtitle for {video_id} to {expected_srt_path}")
                if progress_callback: progress_callback(f"{lang_code} subs OK for {video_id}", 40 + (langs_to_try.index(lang_code) * 20))
            else:
                # Check output for "no subtitles" messages
                if "has no subtitles" in stderr_subs.lower() or "has no auto captions" in stderr_subs.lower() or \
                   "has no subtitles" in stdout_subs.lower() or "has no auto captions" in stdout_subs.lower() or \
                   (process_subs.returncode !=0 and "subtitles" in stderr_subs.lower()): # Generic catch for subtitle issues
                    print(f"No {lang_code} subtitles available for {video_id}. yt-dlp: {stderr_subs.strip()}")
                    if progress_callback: progress_callback(f"No {lang_code} subs for {video_id}", 40 + (langs_to_try.index(lang_code) * 20) )
                else: # Other error
                    print(f"Error downloading {lang_code} subtitles for {video_id}. RC: {process_subs.returncode}. Stderr: {stderr_subs.strip()}. Stdout: {stdout_subs.strip()}")
                    # Do not set path if error
                video_info[f'subtitle_file_path_{lang_code}'] = None


        except subprocess.TimeoutExpired:
            print(f"Timeout downloading {lang_code} subtitles for {video_id}")
            video_info[f'subtitle_file_path_{lang_code}'] = None
            if progress_callback: progress_callback(f"Timeout {lang_code} subs for {video_id}", 40 + (langs_to_try.index(lang_code) * 20) )
        except Exception as e:
            print(f"An exception occurred downloading {lang_code} subtitles for {video_id}: {e}")
            video_info[f'subtitle_file_path_{lang_code}'] = None
            if progress_callback: progress_callback(f"Exception {lang_code} subs for {video_id}", 40 + (langs_to_try.index(lang_code) * 20) )

    # Step 3: Convert downloaded SRTs to plain text
    for lang_code, srt_path in downloaded_srt_files.items():
        if srt_path and os.path.exists(srt_path):
            if progress_callback:
                progress_callback(f"Converting {lang_code} SRT for {video_id}...", 70 + (langs_to_try.index(lang_code) * 10) )
            
            plain_text_filename = f"{video_id}_{lang_code}_plain.txt"
            plain_text_filepath = os.path.join(text_subs_dir, plain_text_filename)
            
            text_content, error_msg = convert_srt_to_plain_text(srt_path)
            
            if error_msg:
                print(f"Error converting {lang_code} SRT to text for {video_id}: {error_msg}")
                video_info[f'plain_text_subtitle_path_{lang_code}'] = None
            else:
                try:
                    with open(plain_text_filepath, 'w', encoding='utf-8') as f_text:
                        f_text.write(text_content)
                    video_info[f'plain_text_subtitle_path_{lang_code}'] = plain_text_filepath
                    print(f"Successfully converted {lang_code} SRT to text: {plain_text_filepath}")
                    if progress_callback: progress_callback(f"Text {lang_code} OK for {video_id}", 80 + (langs_to_try.index(lang_code) * 10))
                except IOError as e:
                    print(f"IOError writing plain text subtitle for {lang_code}, {video_id}: {e}")
                    video_info[f'plain_text_subtitle_path_{lang_code}'] = None
        else:
            video_info[f'plain_text_subtitle_path_{lang_code}'] = None # Ensure key exists even if no srt
            
    if progress_callback:
        progress_callback(f"Processing complete for {video_id}", 100)
    return video_info


def process_playlist(playlist_url, db_path, subtitle_base_dir, progress_callback=None):
    """
    Processes a YouTube playlist: fetches video info, downloads/converts subtitles, and saves to DB.

    Args:
        playlist_url (str): The URL of the YouTube playlist.
        db_path (str): Path to the SQLite database file.
        subtitle_base_dir (str): Base directory to store raw and text subtitles.
                                 (e.g., databases/db_name_subtitles/)
        progress_callback (function, optional): Callback for overall progress and individual video status.
                                                Receives (message_type, data)
                                                message_type: 'total_videos', 'video_processing_start', 
                                                              'video_progress', 'video_completed', 'video_error',
                                                              'all_completed'
                                                data: details for the message type.
    """
    if progress_callback:
        progress_callback("playlist_start", {"playlist_url": playlist_url})

    conn = create_db_connection(db_path)
    if not conn:
        print(f"Cannot connect to database at {db_path}")
        if progress_callback: progress_callback("error", {"message": f"Cannot connect to DB: {db_path}"})
        return

    # Ensure subtitle directories exist for this database
    # Example: databases/my_playlist_db/subtitles/raw_subtitles
    #          databases/my_playlist_db/subtitles/text_subtitles
    # This implies subtitle_base_dir is specific to this DB, e.g., os.path.join(os.path.dirname(db_path), f"{os.path.splitext(os.path.basename(db_path))[0]}_subtitles")
    
    # For now, let's assume subtitle_base_dir is correctly passed as something like:
    # <app_data_dir>/<db_name_without_ext>_subtitles/
    # And fetch_video_details_and_subtitles will create raw_subtitles and text_subtitles inside it.
    os.makedirs(subtitle_base_dir, exist_ok=True)


    # Step 1: Get all video IDs/URLs from the playlist
    if progress_callback:
        progress_callback("playlist_fetch_items", {"status": "Fetching video list from playlist..."})
    
    # Command to extract basic info (ID, title) for all videos in a playlist
    # Using --flat-playlist to quickly get item list without full metadata for each yet.
    # Then, --dump-json to parse it.
    cmd_playlist = [
        'yt-dlp',
        '--flat-playlist', # Don't extract individual video info yet
        '-J',              # Dump playlist info as JSON (shorthand for --dump-json)
        '--no-warnings',
        playlist_url
    ]

    video_entries = []
    try:
        process_playlist_meta = subprocess.Popen(cmd_playlist, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        stdout_playlist, stderr_playlist = process_playlist_meta.communicate(timeout=120) # 2 min timeout

        if process_playlist_meta.returncode == 0 and stdout_playlist:
            playlist_data = json.loads(stdout_playlist)
            if 'entries' in playlist_data:
                video_entries = playlist_data['entries'] # List of dicts, each has 'id', 'url', 'title'
                if progress_callback:
                    progress_callback("playlist_fetch_items_success", {"count": len(video_entries), "raw_data_snippet": video_entries[:2]}) # Send snippet
            else:
                # Handle cases where it's a single video URL passed as a playlist
                if playlist_data.get('_type') != 'playlist': # It might be a single video JSON
                     video_entries = [playlist_data] # Treat as a playlist of one
                else: # Actual playlist but no entries
                    print(f"Playlist {playlist_url} has no video entries or failed to parse entries.")
                    if progress_callback: progress_callback("error", {"message": f"Playlist has no entries or parse error. Output: {stdout_playlist[:200]}"})
                    return
        else:
            print(f"Error fetching playlist items from {playlist_url}: {stderr_playlist}")
            if progress_callback: progress_callback("error", {"message": f"yt-dlp error fetching playlist items: {stderr_playlist}"})
            conn.close()
            return
            
    except subprocess.TimeoutExpired:
        print(f"Timeout fetching playlist items from {playlist_url}")
        if progress_callback: progress_callback("error", {"message": "Timeout fetching playlist items."})
        conn.close()
        return
    except json.JSONDecodeError:
        print(f"Error decoding JSON from playlist {playlist_url}. Output: {stdout_playlist}")
        if progress_callback: progress_callback("error", {"message": f"Error decoding playlist JSON. Output: {stdout_playlist[:200]}"})
        conn.close()
        return
    except Exception as e:
        print(f"An exception occurred fetching playlist items {playlist_url}: {e}")
        if progress_callback: progress_callback("error", {"message": f"Exception fetching playlist items: {str(e)}"})
        conn.close()
        return

    if not video_entries:
        print(f"No videos found in playlist {playlist_url}.")
        if progress_callback: progress_callback("playlist_empty", {"message": "No videos found in the playlist."})
        conn.close()
        return

    total_videos = len(video_entries)
    if progress_callback:
        progress_callback("total_videos", {"count": total_videos})

    # Step 2: Process each video
    for index, entry in enumerate(video_entries):
        video_id = entry.get('id')
        video_title_playlist = entry.get('title', 'Unknown Title') # Title from playlist dump
        video_url_playlist = entry.get('url') # This might be relative, better use ID

        if not video_id:
            print(f"Skipping entry without ID: {entry}")
            if progress_callback: progress_callback("video_error", {"index": index, "title": video_title_playlist, "error": "Missing video ID in playlist entry"})
            continue

        if progress_callback:
            progress_callback("video_processing_start", {"index": index, "total": total_videos, "video_id": video_id, "title": video_title_playlist})

        # Define a per-video progress callback if needed, or pass the main one
        def single_video_progress_callback(task_description, percentage):
            if progress_callback:
                progress_callback("video_progress", {
                    "index": index, "total": total_videos, "video_id": video_id, 
                    "task": task_description, "percentage": percentage
                })
        
        video_details = fetch_video_details_and_subtitles(video_id, subtitle_base_dir, single_video_progress_callback)

        if video_details:
            # Add to database
            db_id = add_video_to_database(conn, video_details)
            if db_id:
                print(f"Successfully processed and added video {video_details.get('title', video_id)} to DB (ID: {db_id}).")
                if progress_callback:
                    progress_callback("video_completed", {"index": index, "total": total_videos, "video_id": video_id, "db_id": db_id, "details": video_details})
            else:
                print(f"Failed to add video {video_details.get('title', video_id)} to database.")
                if progress_callback:
                    progress_callback("video_error", {"index": index, "total": total_videos, "video_id": video_id, "error": "Failed to add to database"})
        else:
            print(f"Failed to process video details for {video_id} ({video_title_playlist}).")
            if progress_callback:
                progress_callback("video_error", {"index": index, "total": total_videos, "video_id": video_id, "error": "Failed to fetch/process video details and subtitles."})
    
    if progress_callback:
        progress_callback("all_completed", {"total_processed": total_videos}) # Can add more stats here
    
    conn.close()
    print(f"Finished processing playlist: {playlist_url}")


if __name__ == '__main__':
    # Example Usage:
    # Ensure you have a database initialized with the 'videos' table (see database_manager.py)
    # python playlist_processor.py <playlist_url> <db_path> <subtitle_storage_base_dir>
    
    import sys
    if len(sys.argv) < 4:
        print("Usage: python playlist_processor.py <playlist_url> <db_path> <subtitle_storage_base_dir>")
        print("Example: python playlist_processor.py \"https://www.youtube.com/playlist?list=YOUR_PLAYLIST_ID\" \"../databases/test_playlist.db\" \"../databases/test_playlist_subtitles\"")
        sys.exit(1)

    playlist_url_arg = sys.argv[1]
    db_path_arg = sys.argv[2]
    subtitle_dir_arg = sys.argv[3]

    # Create dummy DB and table for testing if it doesn't exist
    if not os.path.exists(db_path_arg):
        print(f"Database {db_path_arg} not found. Creating a dummy one for this test.")
        conn_test = create_db_connection(db_path_arg)
        if conn_test:
            # Simplified table creation for testing, refer to database_manager.py for full schema
            # This is just to make the script runnable for a quick test.
            # IN A REAL SCENARIO, THE DB SHOULD BE PROPERLY INITIALIZED FIRST.
            try:
                cursor = conn_test.cursor()
                # Must match columns in add_video_to_database
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT UNIQUE NOT NULL, video_url TEXT, channel_id TEXT, title TEXT, published_at TIMESTAMP,
                    subtitle_status TEXT, subtitle_file_path TEXT, plain_text_subtitle_path TEXT,
                    subtitle_fetched_at TIMESTAMP, subtitle_to_text_status TEXT, subtitle_to_text_completed_at TIMESTAMP,
                    source_script TEXT, status TEXT, last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                ''')
                conn_test.commit()
                print(f"Dummy 'videos' table created in {db_path_arg} for testing.")
            except sqlite3.Error as e:
                print(f"Error creating dummy table: {e}")
            finally:
                if conn_test: conn_test.close()
        else:
            print(f"Failed to create dummy DB {db_path_arg}. Exiting.")
            sys.exit(1)


    def demo_progress_callback(message_type, data):
        print(f"[Progress Callback] Type: {message_type}, Data: {data}")

    print(f"Starting playlist processing for: {playlist_url_arg}")
    process_playlist(playlist_url_arg, db_path_arg, subtitle_dir_arg, demo_progress_callback)
    print("Playlist processing script finished.") 