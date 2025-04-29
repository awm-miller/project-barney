import sqlite3
import json
import os
import zipfile
import html
import argparse
from pathlib import Path
import logging
from datetime import datetime
import re
import math
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()

# --- Configuration ---
LOG_FILE = "generate_report.log"

# Get required paths from environment variables
DATABASE_PATH = os.getenv("DATABASE_PATH")
if not DATABASE_PATH:
    raise ValueError("DATABASE_PATH not found in .env file. Please set it to your database file path.")

ANALYSIS_DIR = os.getenv("ANALYSIS_DIR")
if not ANALYSIS_DIR:
    raise ValueError("ANALYSIS_DIR not found in .env file. Please set it to your desired analysis output path.")

TRANSCRIPTS_DIR = os.getenv("TRANSCRIPTS_DIR")
if not TRANSCRIPTS_DIR:
    raise ValueError("TRANSCRIPTS_DIR not found in .env file. Please set it to your desired transcript output path.")

# Defaults for output files
DEFAULT_OUTPUT_HTML_NAME = "report.html"
DEFAULT_OUTPUT_ZIP_NAME = "hate_preachers_report_archive.zip"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_db_connection(db_path: Path) -> sqlite3.Connection:
    """Establishes a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        logging.info(f"Connected to database: {db_path}")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database {db_path}: {e}")
        raise

def fetch_video_data(conn: sqlite3.Connection) -> list:
    """Fetches relevant video data from the database."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                id,
                video_title,
                channel_title,
                video_url,
                video_path,
                transcript_path,
                ai_analysis_path,
                ai_analysis_content,
                status
            FROM video_processing
            ORDER BY channel_title, video_title
        """)
        data = [dict(row) for row in cursor.fetchall()]
        logging.info(f"Fetched data for {len(data)} videos from the database.")
        return data
    except sqlite3.Error as e:
        logging.error(f"Error fetching data from database: {e}")
        return []

def read_file_content(file_path: Path, encoding='utf-8') -> str | None:
    """Reads the content of a file."""
    if not file_path or not file_path.exists():
        logging.warning(f"File not found or path is invalid: {file_path}")
        return None
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            return f.read()
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None

def parse_analysis(analysis_path: Path | None, analysis_content: str | None) -> dict | None:
    """Parses AI analysis data, preferring file path then content string."""
    content = None
    source = None
    if analysis_path and analysis_path.exists():
        content = read_file_content(analysis_path)
        source = analysis_path
    elif analysis_content:
        content = analysis_content
        source = "database content"
    else:
        logging.warning("No valid analysis path or content found.")
        return None

    if content:
        try:
            # Handle potential markdown code blocks
            if content.strip().startswith("```"):
                 # Find the first '{' or '[' to mark the start of JSON
                json_start_index = content.find('{')
                if json_start_index == -1:
                    json_start_index = content.find('[')

                # Find the last '}' or ']' to mark the end of JSON
                json_end_index = content.rfind('}')
                if json_end_index == -1:
                    json_end_index = content.rfind(']')

                if json_start_index != -1 and json_end_index != -1:
                    content = content[json_start_index : json_end_index + 1]
                else:
                    # Fallback: remove ```json and ``` if present
                    content = content.replace("```json", "").replace("```", "").strip()


            return json.loads(content)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from {source}: {e}")
            logging.debug(f"Problematic content: {content[:500]}...") # Log snippet
            return None
    return None

def format_seconds(total_seconds: float) -> str:
    """Converts seconds into HH:MM:SS or MM:SS format."""
    if not isinstance(total_seconds, (int, float)) or total_seconds < 0:
        return "N/A"
    try:
        total_seconds = float(total_seconds)
        if math.isnan(total_seconds) or math.isinf(total_seconds):
             return "N/A"

        seconds_int = int(total_seconds)
        hours = seconds_int // 3600
        minutes = (seconds_int % 3600) // 60
        seconds = seconds_int % 60

        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes:02d}:{seconds:02d}"
    except Exception as e:
        logging.error(f"Error formatting seconds '{total_seconds}': {e}")
        return "N/A"

def generate_html_report(data: list, analysis_dir: Path, transcripts_dir: Path, output_zip_name: str) -> str:
    """Generates the HTML report string."""
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sermon Analysis Report</title>
    <style>
        body {{ font-family: sans-serif; line-height: 1.6; padding: 20px; background-color: #f4f4f4; color: #333; }}
        .container {{ max-width: 1200px; margin: auto; background: #fff; padding: 30px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
        h1 {{ text-align: center; color: #444; margin-bottom: 30px; }}
        .video-entry-details {{ margin-bottom: 20px; border: 1px solid #ddd; border-radius: 5px; background-color: #fdfdfd; }}
        .video-entry-details summary {{
            cursor: pointer;
            padding: 15px 20px;
            font-weight: bold;
            font-size: 1.2em;
            background-color: #f1f1f1;
            border-bottom: 1px solid #ddd;
            border-radius: 5px 5px 0 0;
            list-style: none; /* Remove default marker */
            position: relative;
        }}
        .video-entry-details summary::before {{
            content: '►'; /* Collapsed marker */
            position: absolute;
            left: 10px;
            top: 50%;
            transform: translateY(-50%);
            font-size: 0.8em;
            margin-right: 8px;
        }}
        .video-entry-details[open] summary::before {{
            content: '▼'; /* Expanded marker */
        }}
        .video-entry-details[open] summary {{
             border-bottom: 1px solid #ddd;
             border-radius: 5px 5px 0 0;
        }}
        .video-entry-content {{ padding: 20px; }}
        h2 {{ color: #555; border-bottom: 2px solid #eee; padding-bottom: 5px; margin-top: 0; }}
        h3 {{ color: #666; }}
        .flags-table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        .flags-table th, .flags-table td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: left; vertical-align: top; }}
        .flags-table th {{ background-color: #e9e9e9; font-weight: bold; }}
        .flag-quote {{ font-family: monospace; background-color: #eee; padding: 2px 4px; border-radius: 3px; white-space: pre-wrap; word-wrap: break-word; }}
        .transcript-details summary {{ cursor: pointer; font-weight: bold; color: #007bff; margin-top: 15px; }}
        .transcript-content {{ margin-top: 10px; padding: 15px; background-color: #f9f9f9; border: 1px solid #eee; border-radius: 4px; white-space: pre-wrap; word-wrap: break-word; max-height: 400px; overflow-y: auto; }}
        .status {{ font-style: italic; color: #888; }}
        .no-flags {{ color: green; font-weight: bold; margin-top: 10px; }}
        .analysis-error {{ color: red; font-weight: bold; margin-top: 10px; }}
        .file-missing {{ color: orange; font-style: italic; }}
        .download-section {{ text-align: center; margin-bottom: 30px; padding: 15px; background-color: #e7f3ff; border: 1px solid #b3d7ff; border-radius: 5px; }}
        .download-button {{ display: inline-block; padding: 12px 25px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px; font-size: 1.1em; font-weight: bold; transition: background-color 0.3s ease; }}
        .download-button:hover {{ background-color: #0056b3; }}
        a {{ color: #007bff; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Sermon Analysis Report</h1>

        <div class="download-section">
            <p>Download all source materials (videos, transcripts, analysis, database, report) as a single archive:</p>
            <a href="{html.escape(output_zip_name)}" download class="download-button">Download Everything (.zip)</a>
        </div>

        {''.join(generate_video_entry_html(video, analysis_dir, transcripts_dir) for video in data)}
    </div>
</body>
</html>
"""
    logging.info("Generated HTML content.")
    return html_content

def generate_video_entry_html(video: dict, analysis_dir: Path, transcripts_dir: Path) -> str:
    """Generates HTML for a single video entry."""
    video_id = video['id']
    title = html.escape(video.get('video_title', 'N/A'))
    channel = html.escape(video.get('channel_title', 'N/A'))
    # url = html.escape(video.get('video_url', '#')) # Keep original URL for reference maybe?
    status = html.escape(video.get('status', 'N/A')) # Still needed for logic, just not displayed directly
    analysis_path = Path(video['ai_analysis_path']) if video.get('ai_analysis_path') else None
    transcript_path = Path(video['transcript_path']) if video.get('transcript_path') else None
    video_local_path_str = video.get('video_path')
    video_local_path = Path(video_local_path_str) if video_local_path_str else None

    # --- Analysis ---
    analysis_data = parse_analysis(analysis_path, video.get('ai_analysis_content'))
    analysis_html = ""
    if status == 'analysis_complete' and analysis_data:
        summary = html.escape(analysis_data.get('summary', 'No summary provided.'))
        flags = analysis_data.get('flags', [])
        analysis_html += f"<p><b>Analysis Summary:</b> {summary}</p>"
        if flags:
            analysis_html += """
            <table class="flags-table">
                <thead>
                    <tr><th>Time Range</th><th>Category</th><th>Quote</th><th>Reason</th></tr>
                </thead>
                <tbody>"""
            for flag in flags:
                # ts = html.escape(flag.get('timestamp', 'N/A')) # Original timestamp field (often start time)
                cat = html.escape(flag.get('category', 'N/A'))
                quote = html.escape(flag.get('quote', 'N/A'))
                reason = html.escape(flag.get('brief_reason', 'N/A'))

                # --- Extract and format time range from quote --- 
                time_display = "N/A"
                raw_quote_for_regex = flag.get('quote', '') # Use unescaped quote for regex
                match = re.search(r'\[\s*(\d+\.?\d*)[sS]?\s*-\s*(\d+\.?\d*)[sS]?\s*\]', raw_quote_for_regex)
                if match:
                    try:
                        start_sec = float(match.group(1))
                        end_sec = float(match.group(2))
                        start_formatted = format_seconds(start_sec)
                        end_formatted = format_seconds(end_sec)
                        if start_formatted != "N/A" and end_formatted != "N/A":
                             time_display = f"{start_formatted} - {end_formatted}"
                        else:
                             logging.warning(f"Could not format extracted seconds: {match.group(1)}, {match.group(2)}")
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Could not parse extracted seconds '{match.group(1)}' or '{match.group(2)}': {e}")
                else:
                    # Fallback: try parsing the original timestamp field if regex fails
                    original_ts = flag.get('timestamp', 'N/A')
                    if original_ts != 'N/A':
                         # Attempt to parse HH:MM:SS.ms or seconds
                         try:
                             parts = original_ts.split(':')
                             if len(parts) == 3: # HH:MM:SS.ms format
                                 sec = float(parts[2])
                                 time_display = format_seconds(sec + int(parts[1])*60 + int(parts[0])*3600)
                             elif len(parts) == 1: # Assume seconds
                                 time_display = format_seconds(float(original_ts))
                             else:
                                 time_display = html.escape(original_ts) # Display as is if format unknown
                         except (ValueError, TypeError):
                             time_display = html.escape(original_ts) # Display as is if parsing fails

                analysis_html += f"""
                    <tr>
                        <td>{time_display}</td>
                        <td>{cat}</td>
                        <td><code class="flag-quote">{quote}</code></td>
                        <td>{reason}</td>
                    </tr>"""
            analysis_html += "</tbody></table>"
        else:
            analysis_html += '<p class="no-flags">No concerning passages flagged by AI.</p>'
    elif status == 'analysis_failed':
         analysis_html += '<p class="analysis-error">AI analysis failed for this video.</p>'
    elif status in ['transcription_complete', 'analysis_pending']:
         # Display pending/transcribed status subtly if needed, or remove entirely
         # analysis_html += f'<p class="status">AI analysis pending (Status: {status}).</p>'
         analysis_html += '<p class="status">AI analysis not yet complete.</p>' # Simplified message
    elif not analysis_path and not video.get('ai_analysis_content'):
         # analysis_html += f'<p class="status">AI analysis not available (Status: {status}).</p>'
         analysis_html += '<p class="status">AI analysis not available.</p>' # Simplified message
    else: # Catch case where status is complete but parsing failed
         # analysis_html += f'<p class="analysis-error">Could not load or parse AI analysis data (Status: {status}).</p>'
         analysis_html += '<p class="analysis-error">Could not load or parse AI analysis data.</p>' # Simplified message

    # --- Transcript ---
    transcript_html = ""
    if transcript_path and transcript_path.exists():
        transcript_content = read_file_content(transcript_path)
        if transcript_content:
            transcript_html = f"""
            <details class="transcript-details">
                <summary>Show/Hide Full Transcript</summary>
                <div class="transcript-content">{html.escape(transcript_content)}</div>
            </details>
            """
        else:
             transcript_html = f'<p class="file-missing">Could not read transcript file: {html.escape(str(transcript_path))}</p>'
    elif video.get('transcript_path'):
         transcript_html = f'<p class="file-missing">Transcript file not found: {html.escape(video.get("transcript_path"))}</p>'
    else:
         transcript_html = '<p class="status">Transcript not available.</p>'


    # --- Video Link ---
    # Link to local file relative to the HTML report
    video_link_html = '<span>No Local Video Path</span>'
    if video_local_path:
        relative_video_path = video_local_path.name # Assumes video is in same dir as report.html
        # Check if file actually exists where expected (relative to base_dir)
        expected_local_path = analysis_dir / relative_video_path
        if expected_local_path.exists() and expected_local_path.is_file():
             video_link_html = f'<a href="{html.escape(relative_video_path)}" target="_blank">Watch Local Video File</a>'
        else:
             logging.warning(f"Local video file not found at expected location: {expected_local_path} (Referenced as: {video_local_path})")
             video_link_html = f'<span>Local Video File Not Found ({html.escape(relative_video_path)})</span>'
    else:
         logging.warning(f"No video_path found in database for video ID {video_id}")

    # Optionally, include the original YouTube URL as well
    original_url = html.escape(video.get('video_url', '#'))
    original_url_html = f' | <a href="{original_url}" target="_blank" rel="noopener noreferrer">Watch on YouTube</a>' if original_url != '#' else ''

    return f"""
        <details class="video-entry-details">
            <summary>{title} - <i>{channel}</i></summary>
            <div class="video-entry-content">
                <h2>{title}</h2>
                <h3>Channel: {channel}</h3>
                <p>{video_link_html}{original_url_html}</p>
                <!-- <p><i>Status: {status}</i></p>  <-- REMOVED -->

                <h3>AI Analysis</h3>
                {analysis_html}

                <h3>Transcript</h3>
                {transcript_html}
            </div>
        </details>
        """

def add_to_zip(zipf: zipfile.ZipFile, file_path: Path, arc_path: Path | str):
    """Adds a file or directory to the zip archive."""
    try:
        if file_path.is_file():
            zipf.write(file_path, arc_path)
            # logging.debug(f"Added file to zip: {file_path} as {arc_path}")
        elif file_path.is_dir():
            for root, _, files in os.walk(file_path):
                for file in files:
                    full_path = Path(root) / file
                    relative_path = full_path.relative_to(file_path.parent) # Keep the directory name in arcname
                    zipf.write(full_path, relative_path)
                    # logging.debug(f"Added file to zip: {full_path} as {relative_path}")
        else:
            logging.warning(f"Item not found, cannot add to zip: {file_path}")
    except Exception as e:
        logging.error(f"Error adding {file_path} to zip as {arc_path}: {e}")


def create_zip_archive(
    output_zip_path: Path,
    db_path: Path,
    analysis_dir: Path,
    transcripts_dir: Path,
    output_html_path: Path,
    video_data: list
):
    """Creates a zip archive containing the report and associated files."""
    logging.info(f"Creating zip archive: {output_zip_path}")
    try:
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add HTML report
            add_to_zip(zipf, output_html_path, output_html_path.name)

            # Add Database
            if db_path.exists():
                add_to_zip(zipf, db_path, db_path.name)
            else:
                logging.warning(f"Database file not found, not adding to zip: {db_path}")

            # Add Transcript and Analysis files mentioned in video_data
            added_transcripts = set()
            added_analyses = set()

            for video in video_data:
                # Add Transcript
                transcript_path_str = video.get('transcript_path')
                if transcript_path_str and transcript_path_str not in added_transcripts:
                    transcript_path = Path(transcript_path_str)
                    if transcript_path.exists() and transcript_path.is_relative_to(transcripts_dir):
                         arc_path = transcript_path.relative_to(transcripts_dir)
                         add_to_zip(zipf, transcript_path, Path(transcripts_dir.name) / arc_path)
                         added_transcripts.add(transcript_path_str)
                    elif transcript_path.exists(): # Add even if not relative, using basename
                         add_to_zip(zipf, transcript_path, Path(transcripts_dir.name) / transcript_path.name)
                         added_transcripts.add(transcript_path_str)
                         logging.warning(f"Transcript added with basename as it wasn't relative to specified dir: {transcript_path}")

                # Add Analysis
                analysis_path_str = video.get('ai_analysis_path')
                if analysis_path_str and analysis_path_str not in added_analyses:
                    analysis_path = Path(analysis_path_str)
                    if analysis_path.exists() and analysis_path.is_relative_to(analysis_dir):
                        arc_path = analysis_path.relative_to(analysis_dir)
                        add_to_zip(zipf, analysis_path, Path(analysis_dir.name) / arc_path)
                        added_analyses.add(analysis_path_str)
                    elif analysis_path.exists():
                        add_to_zip(zipf, analysis_path, Path(analysis_dir.name) / analysis_path.name)
                        added_analyses.add(analysis_path_str)
                        logging.warning(f"Analysis file added with basename as it wasn't relative to specified dir: {analysis_path}")
                        
            # Optionally add video files (can make zip very large)
            # for video in video_data:
            #     video_path_str = video.get('video_path')
            #     if video_path_str:
            #         video_path = Path(video_path_str)
            #         if video_path.exists():
            #             # Decide on an archive path structure, e.g., 'videos/video_name.mp4'
            #             add_to_zip(zipf, video_path, Path('videos') / video_path.name)

        logging.info(f"Successfully created zip archive: {output_zip_path}")

    except Exception as e:
        logging.error(f"Error creating zip archive {output_zip_path}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Generate an HTML report and zip archive from transcription analysis data.")
    # Removed base_dir, using specific paths from env vars now
    parser.add_argument(
        "--db-path",
        default=DATABASE_PATH, # Default comes from env var
        help="Path to the SQLite database file. Overrides DATABASE_PATH environment variable."
    )
    parser.add_argument(
        "--analysis-dir",
        default=ANALYSIS_DIR, # Default comes from env var
        help="Directory containing analysis JSON files. Overrides ANALYSIS_DIR environment variable."
    )
    parser.add_argument(
        "--transcripts-dir",
        default=TRANSCRIPTS_DIR, # Default comes from env var
        help="Directory containing transcript text files. Overrides TRANSCRIPTS_DIR environment variable."
    )
    parser.add_argument(
        "--output-html",
        default=DEFAULT_OUTPUT_HTML_NAME,
        help=f"Filename for the output HTML report. Default: {DEFAULT_OUTPUT_HTML_NAME}"
    )
    parser.add_argument(
        "--output-zip",
        default=DEFAULT_OUTPUT_ZIP_NAME,
        help=f"Filename for the output ZIP archive. Default: {DEFAULT_OUTPUT_ZIP_NAME}"
    )

    args = parser.parse_args()

    db_path = Path(args.db_path)
    analysis_dir = Path(args.analysis_dir)
    transcripts_dir = Path(args.transcripts_dir)
    output_html_path = Path(args.output_html).resolve() # Resolve to get absolute path for zip creation
    output_zip_path = Path(args.output_zip).resolve()

    # Ensure output directories exist for the report/zip if they are not in cwd
    output_html_path.parent.mkdir(parents=True, exist_ok=True)
    output_zip_path.parent.mkdir(parents=True, exist_ok=True)

    conn = None
    try:
        conn = get_db_connection(db_path)
        video_data = fetch_video_data(conn)

        if not video_data:
            logging.warning("No video data found in the database. Report will be empty.")
            # Create an empty report maybe?
            html_report_content = "<html><body><h1>Sermon Analysis Report</h1><p>No data found in database.</p></body></html>"
        else:
            html_report_content = generate_html_report(video_data, analysis_dir, transcripts_dir, output_zip_path.name)

        # Write HTML report
        logging.info(f"Writing HTML report to: {output_html_path}")
        try:
            with open(output_html_path, 'w', encoding='utf-8') as f:
                f.write(html_report_content)
            logging.info("Successfully wrote HTML report.")
        except IOError as e:
            logging.error(f"Error writing HTML report to {output_html_path}: {e}")

        # Create ZIP archive
        create_zip_archive(
            output_zip_path,
            db_path,
            analysis_dir,
            transcripts_dir,
            output_html_path,
            video_data
        )

    except Exception as e:
        logging.error(f"An unexpected error occurred during report generation: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    logging.info("Report generation process finished.")

if __name__ == "__main__":
    main() 