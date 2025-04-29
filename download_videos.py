import os
import csv
import logging
import argparse
import random
import time
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import yt_dlp

# --- Configuration ---
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    raise ValueError("YOUTUBE_API_KEY not found in .env file or environment variables.")

# Get required download directory from environment variable
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")
if not DOWNLOAD_DIR:
    raise ValueError("DOWNLOAD_DIR not found in .env file or environment variables. Please set it to your desired video download path.")

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

DEFAULT_SEARCH_CSV = "search_results.csv"
DEFAULT_DATES_FILE = "target_dates.txt"
DEFAULT_OUTPUT_CSV = "download_results.csv"
LOG_FILE = "download_videos.log"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,  # Changed back to INFO from DEBUG
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# --- Helper Functions ---
def initialize_youtube_api():
    """Initializes and returns the YouTube Data API client."""
    start_time = time.time()
    api_client = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=YOUTUBE_API_KEY)
    elapsed = time.time() - start_time
    logging.info(f"YouTube API initialized in {elapsed:.2f} seconds")
    return api_client


def sanitize_filename(name: str) -> str:
    """Sanitize string to be safe for filenames."""
    # Replace invalid characters with '_'
    return "".join(c if c.isalnum() or c in ' ._-' else '_' for c in name).strip()


def find_closest_video(youtube, channel_id: str, target_date_str: str, window_days: int = 7):
    """
    Finds the video published closest to target_date within a fixed window for the given channel.
    Returns (video_id, video_title, published_at) or (None, None, error_msg).
    """
    start_time = time.time()
    logging.info(f"Searching for video near {target_date_str} (±{window_days} days) for channel {channel_id}")

    try:
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    except ValueError as e:
        return None, None, f"Invalid date format: {target_date_str}"

    # Define the fixed search window
    window_before = (target_date - timedelta(days=window_days)).isoformat() + 'Z'
    window_after = (target_date + timedelta(days=window_days)).isoformat() + 'Z'

    try:
        search_response = youtube.search().list(
            channelId=channel_id,
            part="snippet",
            type="video",
            order="date",
            publishedAfter=window_before,
            publishedBefore=window_after,
            maxResults=50
        ).execute()
    except HttpError as e:
        elapsed = time.time() - start_time
        logging.error(f"HTTP error fetching videos for channel {channel_id} near {target_date_str}: {e} ({elapsed:.2f}s)")

        # Handle quota errors with clear message and immediate return
        error_content = str(e).lower()
        if "quota" in error_content or "quotaexceeded" in error_content:
            logging.critical("YouTube API QUOTA EXCEEDED during search.")
            return None, None, "API QUOTA EXCEEDED"

        return None, None, f"API Error: {e.status_code}"
    except Exception as e:
        elapsed = time.time() - start_time
        logging.error(f"Unexpected error fetching videos for channel {channel_id} near {target_date_str}: {e} ({elapsed:.2f}s)")
        return None, None, str(e)

    items = search_response.get("items", [])
    if items:
        # Found videos, find the closest one
        closest = None
        min_diff = None
        for item in items:
            pub_str = item["snippet"]["publishedAt"]
            pub_date = datetime.fromisoformat(pub_str.rstrip('Z'))
            diff = abs((pub_date - target_date).days)
            if min_diff is None or diff < min_diff:
                min_diff = diff
                closest = (item["id"]["videoId"], item["snippet"]["title"], pub_str)

        video_id, video_title, published_at = closest
        elapsed = time.time() - start_time
        days_from_target = min_diff if min_diff is not None else "unknown"
        logging.info(f"Found video {video_id} (published {published_at}, {days_from_target} days from target) in {elapsed:.2f}s")
        return video_id, video_title, published_at
    else:
        # No videos found in the fixed window
        elapsed = time.time() - start_time
        logging.warning(f"No videos found for channel {channel_id} within ±{window_days} days of {target_date_str} ({elapsed:.2f}s)")
        return None, None, f"No videos found within ±{window_days} days"


def download_video(video_id: str, institution: str, target_date: str, download_dir: str):
    """Downloads the YouTube video and returns the file path or error message."""
    start_time = time.time()
    url = f"https://www.youtube.com/watch?v={video_id}"
    inst_safe = sanitize_filename(institution.replace(' ', '_'))
    fname = f"{inst_safe}_{target_date}_{video_id}.mp4"
    out_path = os.path.join(download_dir, fname)

    os.makedirs(download_dir, exist_ok=True)
    logging.info(f"Starting download for video {video_id}")

    ydl_opts = {
        'outtmpl': out_path,
        'quiet': False,  # Show output
        'noprogress': False,  # Show progress bar
        'format': 'best[height<=720]'  # Limit to 720p to speed up downloads
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        elapsed = time.time() - start_time
        logging.info(f"Downloaded video {video_id} in {elapsed:.2f}s")
        return out_path, None
    except Exception as e:
        elapsed = time.time() - start_time
        logging.error(f"Failed to download video {video_id}: {e} ({elapsed:.2f}s)")
        return None, str(e)


def format_time_delta(seconds):
    """Format seconds into a readable time format (HH:MM:SS)"""
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


# --- Main Execution ---
def main(search_csv, dates_file, download_dir, output_csv):
    total_start_time = time.time()
    start_datetime = datetime.now()
    logging.info(f"--- Starting Download Videos Script at {start_datetime.strftime('%Y-%m-%d %H:%M:%S')} ---")
    logging.info(f"Search strategy: Find FIRST video within ±7 days of ONE of the target dates.")

    # Load search results
    start_time = time.time()
    try:
        with open(search_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            search_results = [row for row in reader]
        elapsed = time.time() - start_time
        logging.info(f"Loaded {len(search_results)} search results in {elapsed:.2f}s")
    except Exception as e:
        logging.error(f"Error reading search results CSV '{search_csv}': {e}")
        return

    # Load dates
    start_time = time.time()
    try:
        with open(dates_file, 'r', encoding='utf-8') as f:
            dates_list = [line.strip() for line in f if line.strip()]
        elapsed = time.time() - start_time
        logging.info(f"Loaded {len(dates_list)} target dates in {elapsed:.2f}s")
        if not dates_list:
             logging.error("No target dates found in dates file. Exiting.")
             return
    except Exception as e:
        logging.error(f"Error reading dates file '{dates_file}': {e}")
        return

    youtube = initialize_youtube_api()

    # Define new output structure (one video per institution max)
    fieldnames = [
        "INSTITUTION",
        "CHANNEL_ID",
        "TARGET_DATE_USED",
        "VIDEO_ID",
        "PUBLISHED_AT",
        "VIDEO_PATH",
        "DAYS_DIFFERENCE",
        "STATUS", # e.g., "DOWNLOADED", "NO_VIDEO_FOUND", "DOWNLOAD_ERROR", "API_QUOTA_EXCEEDED", "NO_CHANNEL_ID"
        "ERROR_DETAIL"
    ]
    results = []

    # Track progress
    processed_institutions = 0
    total_institutions = len(search_results)

    # Track institutions without videos found
    institutions_without_videos = []

    for i, row in enumerate(search_results):
        inst_start_time = time.time()
        institution = row.get("INSTITUTION")
        channel_id = row.get("CHANNEL_ID")
        logging.info(f"--- Processing [{i+1}/{total_institutions}] {institution} ({channel_id or 'No Channel ID'}) ---")

        out_row = { # Initialize with default/failure values
            "INSTITUTION": institution,
            "CHANNEL_ID": channel_id,
            "TARGET_DATE_USED": None,
            "VIDEO_ID": None,
            "PUBLISHED_AT": None,
            "VIDEO_PATH": None,
            "DAYS_DIFFERENCE": None,
            "STATUS": "PENDING",
            "ERROR_DETAIL": None
        }

        if not channel_id:
            logging.warning(f"Skipping '{institution}' as no channel ID found.")
            out_row["STATUS"] = "NO_CHANNEL_ID"
            out_row["ERROR_DETAIL"] = "Channel ID missing in search results CSV"
            results.append(out_row)
            institutions_without_videos.append({
                "institution": institution,
                "channel_id": channel_id,
                "reason": "No channel ID found",
                "dates_tried": []
            })
            processed_institutions += 1
            continue

        # Randomly select up to 3 dates and shuffle their order
        num_dates_to_try = min(3, len(dates_list))
        selected_dates = random.sample(dates_list, num_dates_to_try)
        random.shuffle(selected_dates) # Shuffle the selected dates

        logging.info(f"Trying up to {num_dates_to_try} dates (random order): {selected_dates}")

        found_video_for_inst = False
        api_quota_hit = False
        dates_tried_info = []

        for date_str in selected_dates:
            date_start_time = time.time()
            logging.info(f"Attempting date: {date_str}")
            vid_id, vid_title, vid_pubdate = find_closest_video(youtube, channel_id, date_str) # Using modified function (window_days=7 default)

            if vid_id:
                logging.info(f"Success! Found video {vid_id} for date {date_str}.")
                found_video_for_inst = True

                # Calculate days difference
                try:
                    target_dt = datetime.strptime(date_str, "%Y-%m-%d")
                    pub_dt = datetime.fromisoformat(vid_pubdate.rstrip('Z'))
                    days_diff = abs((pub_dt - target_dt).days)
                except Exception:
                    days_diff = None # Should not happen if vid_pubdate is valid

                # Attempt download
                video_path, download_err = download_video(vid_id, institution, date_str, download_dir)

                # Update output row for success
                out_row["TARGET_DATE_USED"] = date_str
                out_row["VIDEO_ID"] = vid_id
                out_row["PUBLISHED_AT"] = vid_pubdate
                out_row["VIDEO_PATH"] = video_path
                out_row["DAYS_DIFFERENCE"] = days_diff
                if download_err:
                    out_row["STATUS"] = "DOWNLOAD_ERROR"
                    out_row["ERROR_DETAIL"] = download_err
                    logging.error(f"Download failed for {vid_id}: {download_err}")
                else:
                    out_row["STATUS"] = "DOWNLOADED"
                    logging.info(f"Video {vid_id} downloaded successfully to {video_path}")

                dates_tried_info.append({"date": date_str, "result": "FOUND_AND_DOWNLOADED" if not download_err else "FOUND_DOWNLOAD_ERROR", "video_id": vid_id, "error": download_err})
                date_elapsed = time.time() - date_start_time
                logging.info(f"Processed date {date_str} in {date_elapsed:.2f}s (Found video, stopping search for this institution).")
                break # Stop trying dates for this institution once one is found and processed

            else:
                # Video not found for this date, record the reason (error message from find_closest_video)
                error_msg = vid_pubdate # Error message is returned in the pubdate slot
                logging.warning(f"No video found for date {date_str}. Reason: {error_msg}")
                dates_tried_info.append({"date": date_str, "result": "NOT_FOUND", "video_id": None, "error": error_msg})

                if error_msg == "API QUOTA EXCEEDED":
                    api_quota_hit = True
                    out_row["STATUS"] = "API_QUOTA_EXCEEDED"
                    out_row["ERROR_DETAIL"] = "API quota exceeded during video search."
                    logging.critical("API Quota Exceeded. Halting search for this institution.")
                    break # Stop trying dates if quota hit

                # Small delay before next API call if we didn't find a video
                time.sleep(0.5)

            date_elapsed = time.time() - date_start_time
            logging.info(f"Processed date {date_str} in {date_elapsed:.2f}s (No video found).")


        # After trying all selected dates for the institution
        if not found_video_for_inst:
            if not api_quota_hit:
                logging.warning(f"No video found for {institution} after trying dates: {selected_dates}")
                out_row["STATUS"] = "NO_VIDEO_FOUND"
                out_row["ERROR_DETAIL"] = f"No video found within ±7 days for tried dates: {selected_dates}"
            # (If quota hit, status is already set)

            institutions_without_videos.append({
                "institution": institution,
                "channel_id": channel_id,
                "reason": "API Quota Exceeded" if api_quota_hit else "No videos found",
                "dates_tried": dates_tried_info
            })


        results.append(out_row)
        processed_institutions += 1
        inst_elapsed = time.time() - inst_start_time
        logging.info(f"Completed institution {institution} in {inst_elapsed:.2f}s. Status: {out_row['STATUS']}")

        # Update progress (simpler version without time estimate)
        if processed_institutions > 0:
            percent_complete = (processed_institutions / total_institutions) * 100
            if processed_institutions % 10 == 0 or processed_institutions == total_institutions: # Update every 10 or at the end
                elapsed_so_far = time.time() - total_start_time
                logging.info(f"Progress: {processed_institutions}/{total_institutions} ({percent_complete:.1f}%) institutions processed. Elapsed time: {format_time_delta(elapsed_so_far)}")

        # Optional: Add a slightly longer delay between institutions if needed
        # time.sleep(1.0)


    # Write output CSV
    start_time = time.time()
    try:
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        elapsed = time.time() - start_time
        logging.info(f"Wrote results to '{output_csv}' in {elapsed:.2f}s")
    except Exception as e:
        logging.error(f"Error writing download results CSV '{output_csv}': {e}")

    # Write missing videos summary to a file
    if institutions_without_videos:
        missing_output_filename = f"missing_videos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        try:
            with open(missing_output_filename, 'w', encoding='utf-8') as f:
                f.write(f"--- Institutions Without Downloaded Videos ({len(institutions_without_videos)}) ---\n")
                f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Search CSV: {search_csv}\n")
                f.write(f"Dates File: {dates_file}\n")
                f.write(f"Download Dir: {download_dir}\n")
                f.write(f"Output CSV: {output_csv}\n\n")

                for entry in institutions_without_videos:
                    f.write(f"Institution: {entry['institution']} (Channel: {entry['channel_id'] or 'N/A'})\n")
                    f.write(f"  Reason: {entry['reason']}\n")
                    if entry['dates_tried']:
                        f.write("  Dates Tried:\n")
                        for attempt in entry['dates_tried']:
                             error_info = f", Error: {attempt['error']}" if attempt['error'] else ""
                             f.write(f"    - {attempt['date']}: {attempt['result']}{error_info}\n")
                    f.write("-" * 20 + "\n")
            logging.info(f"Wrote missing videos report to '{missing_output_filename}'")
        except Exception as e:
            logging.error(f"Error writing missing videos report: {e}")

    total_elapsed = time.time() - total_start_time
    end_datetime = datetime.now()

    # Final Summary Logging
    logging.info(f"--- Script Complete ---")
    logging.info(f"Started: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Ended:   {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Total Runtime: {format_time_delta(total_elapsed)} ({total_elapsed:.2f} seconds)")

    # Summarize results from the 'results' list
    downloaded_count = sum(1 for r in results if r["STATUS"] == "DOWNLOADED")
    no_channel_count = sum(1 for r in results if r["STATUS"] == "NO_CHANNEL_ID")
    no_video_count = sum(1 for r in results if r["STATUS"] == "NO_VIDEO_FOUND")
    download_error_count = sum(1 for r in results if r["STATUS"] == "DOWNLOAD_ERROR")
    quota_error_count = sum(1 for r in results if r["STATUS"] == "API_QUOTA_EXCEEDED")

    logging.info(f"--- Results Summary ---")
    logging.info(f"Total Institutions Processed: {total_institutions}")
    logging.info(f"  Successfully Downloaded: {downloaded_count}")
    logging.info(f"  No Channel ID Found:     {no_channel_count}")
    logging.info(f"  No Video Found (±7d):    {no_video_count}")
    logging.info(f"  Download Errors:         {download_error_count}")
    logging.info(f"  API Quota Exceeded:      {quota_error_count}")

    if institutions_without_videos:
        logging.warning(f"({len(institutions_without_videos)} institutions ended without a downloaded video. See '{missing_output_filename}' for details.)")
    else:
        logging.info("Attempted to find/download a video for all institutions.")

    logging.info(f"--- Download Videos Script Finished ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download closest YouTube videos for each institution and dates.")
    parser.add_argument(
        "--search_csv", default=DEFAULT_SEARCH_CSV,
        help=f"CSV file from channel search script (default: {DEFAULT_SEARCH_CSV})"
    )
    parser.add_argument(
        "--dates_file", default=DEFAULT_DATES_FILE,
        help=f"Text file with target dates, one per line (YYYY-MM-DD) (default: {DEFAULT_DATES_FILE})"
    )
    parser.add_argument(
        "--output", default=DEFAULT_OUTPUT_CSV,
        help=f"Path to the output CSV file for download results. Default: {DEFAULT_OUTPUT_CSV}"
    )
    parser.add_argument(
        "--download-dir",
        default=DOWNLOAD_DIR, # Default now comes from env var
        help="Directory to download videos into. Overrides DOWNLOAD_DIR environment variable."
    )

    args = parser.parse_args()

    # Use the directory from args, which defaults to the env var
    main(args.search_csv, args.dates_file, args.download_dir, args.output) 