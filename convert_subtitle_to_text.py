import sqlite3
import argparse
import os
from datetime import datetime
from dotenv import load_dotenv
import webvtt # For parsing VTT files
import re # Added for SRT parsing

# Load environment variables from .env file
load_dotenv()

DATABASE_NAME = "pipeline_database.db"
DEFAULT_PLAIN_TEXT_SUBTITLE_DIR = os.getenv("PLAIN_TEXT_SUBTITLE_DIR", "plain_text_subtitles")
SCRIPT_NAME = "convert_subtitle_to_text.py"

def create_connection(db_file=DATABASE_NAME):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
    return conn

def get_videos_for_conversion(conn, limit=None, job_name=None):
    cursor = conn.cursor()
    table_name = f"videos_{job_name}" if job_name else "videos"
    sql = f"""
    SELECT id, video_id, subtitle_file_path, title
    FROM {table_name}
    WHERE subtitle_status = 'fetched' 
      AND text_source = 'SUBTITLE' 
      AND subtitle_to_text_status = 'pending'
    ORDER BY subtitle_fetched_at ASC
    """
    params = []
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    try:
        cursor.execute(sql, params)
        videos = cursor.fetchall()
        print(f"Found {len(videos)} videos with fetched subtitles pending text conversion from table '{table_name}'.")
        return videos
    except sqlite3.Error as e:
        print(f"Database error fetching videos for subtitle conversion from '{table_name}': {e}")
        return []

def update_video_conversion_status(conn, video_db_id: int, status: str, 
                                   plain_text_path: str = None, error_message: str = None, 
                                   job_name: str = None):
    cursor = conn.cursor()
    table_name = f"videos_{job_name}" if job_name else "videos"
    
    set_clauses = ["subtitle_to_text_status = ?"]
    parameters = [status]
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if status == 'completed':
        set_clauses.append("plain_text_subtitle_path = ?")
        parameters.append(plain_text_path)
        set_clauses.append("subtitle_to_text_completed_at = ?")
        parameters.append(current_time_str)
        set_clauses.append("subtitle_to_text_error_message = NULL")
    elif status == 'failed':
        set_clauses.append("subtitle_to_text_error_message = ?")
        parameters.append(error_message)
        set_clauses.append("plain_text_subtitle_path = NULL")
        set_clauses.append("subtitle_to_text_completed_at = ?") # Record completion time even for failure
        parameters.append(current_time_str)
    
    set_clauses.append("subtitle_to_text_initiated_at = COALESCE(subtitle_to_text_initiated_at, ?)")
    parameters.append(current_time_str)
    set_clauses.append("last_updated_at = ?")
    parameters.append(current_time_str)
    
    sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE id = ?"
    parameters.append(video_db_id)
    
    try:
        cursor.execute(sql, parameters)
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        print(f"DB error updating video (ID: {video_db_id}, Table: {table_name}) subtitle conversion status: {e}")

def convert_vtt_to_plain_text(vtt_file_path):
    """Parses a VTT file and returns all caption text concatenated."""
    if not os.path.exists(vtt_file_path):
        return None, "VTT file not found"
    try:
        captions = webvtt.read(vtt_file_path)
        full_text = "\n".join([caption.text.strip() for caption in captions if caption.text.strip()])
        return full_text, None
    except Exception as e:
        return None, f"Error parsing VTT file {os.path.basename(vtt_file_path)}: {e}"

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

def main():
    parser = argparse.ArgumentParser(description="Convert fetched SRT subtitle files to plain text.")
    parser.add_argument("--limit", type=int, help="Maximum number of subtitles to convert.")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_PLAIN_TEXT_SUBTITLE_DIR,
                        help=f"Directory to save plain text subtitle files. Default: ./{DEFAULT_PLAIN_TEXT_SUBTITLE_DIR}")
    parser.add_argument("--job-name", type=str, default=None,
                        help="Optional job name to operate on a specific table (e.g., videos_my_job).")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    conn = create_connection(DATABASE_NAME)
    if not conn:
        print("Failed to connect to the database. Exiting.")
        return

    videos_to_process = get_videos_for_conversion(conn, args.limit, args.job_name)

    if not videos_to_process:
        print("No videos found needing subtitle-to-text conversion at this time.")
        conn.close()
        return
        
    print(f"Processing {len(videos_to_process)} videos for subtitle to text conversion...")

    for video_row in videos_to_process:
        video_db_id = video_row['id']
        video_id = video_row['video_id']
        subtitle_path = video_row['subtitle_file_path']
        video_title = video_row['title']

        print(f"Converting subtitle for: {video_title} (Video ID: {video_id}, DB ID: {video_db_id})")
        update_video_conversion_status(conn, video_db_id, 'processing', job_name=args.job_name) # Mark as processing (initiated)

        if not subtitle_path or not os.path.exists(subtitle_path):
            err_msg = f"Subtitle file path missing or file not found: {subtitle_path}"
            print(err_msg)
            update_video_conversion_status(conn, video_db_id, 'failed', error_message=err_msg, job_name=args.job_name)
            continue

        plain_text = None
        conversion_error = None

        if subtitle_path.lower().endswith('.srt'):
            plain_text, conversion_error = convert_srt_to_plain_text(subtitle_path)
        else:
            # If fetch_subtitles.py guarantees SRT, this case might become less common
            # or could be an error condition.
            conversion_error = f"Unsupported subtitle file format: {os.path.basename(subtitle_path)}. Only .srt is currently supported for plain text conversion."
            print(conversion_error)
        
        if conversion_error:
            update_video_conversion_status(conn, video_db_id, 'failed', error_message=conversion_error, job_name=args.job_name)
            continue

        if plain_text is not None: # Can be empty string if VTT was valid but had no text
            output_txt_filename = f"{video_id}_plain_subtitle.txt"
            output_txt_path = os.path.join(args.output_dir, output_txt_filename)
            try:
                with open(output_txt_path, 'w', encoding='utf-8') as f:
                    f.write(plain_text)
                print(f"Successfully converted subtitle to text: {output_txt_path}")
                update_video_conversion_status(conn, video_db_id, 'completed', plain_text_path=output_txt_path, job_name=args.job_name)
            except IOError as e:
                err_msg = f"Failed to write plain text subtitle file {output_txt_path}: {e}"
                print(err_msg)
                update_video_conversion_status(conn, video_db_id, 'failed', error_message=err_msg, job_name=args.job_name)
        else:
            # This case means conversion was attempted (no format error) but yielded no text or an error handled by the conversion function.
            # The error should have been captured in conversion_error, but as a fallback:
            err_msg = "Subtitle conversion resulted in no text or an uncaptured error."
            print(err_msg)
            update_video_conversion_status(conn, video_db_id, 'failed', error_message=err_msg, job_name=args.job_name)

    print(f"Finished subtitle to text conversion for {len(videos_to_process)} videos.")
    conn.close()

if __name__ == '__main__':
    main() 