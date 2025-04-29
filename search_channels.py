import os
import csv
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import argparse
import time

# --- Configuration ---
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    raise ValueError("YOUTUBE_API_KEY not found in .env file or environment variables.")

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

DEFAULT_INSTITUTIONS_FILE = "unique_institutions.csv"
DEFAULT_OUTPUT_CSV = "search_results.csv"
LOG_FILE = "search_channels.log"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
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

def search_channel(youtube, institution_name):
    """
    Searches for a YouTube channel for the given institution name.
    Returns the channel ID and title if found, otherwise None, None.
    """
    query = f"{institution_name} official YouTube channel"
    logging.info(f"Searching for channel: '{query}'")
    try:
        search_response = youtube.search().list(
            q=query,
            part="snippet",
            type="channel",
            maxResults=5  # Check top 5 results
        ).execute()

        if not search_response.get("items"):
            logging.warning(f"No channel results found for '{institution_name}'. Trying simpler query.")
            # Try a simpler query as fallback
            query = f"{institution_name} YouTube"
            search_response = youtube.search().list(
                q=query,
                part="snippet",
                type="channel",
                maxResults=1 # Take the top channel result for simpler query
            ).execute()
            if not search_response.get("items"):
                 logging.warning(f"Still no channel results found for '{institution_name}' with simpler query.")
                 return None, None, "No channel found"


        # Heuristic: Prioritize channels with 'official' or the exact name.
        # For simplicity here, we'll take the first result but log its title.
        # A more robust approach would involve checking titles/descriptions.
        first_result = search_response["items"][0]
        channel_id = first_result["snippet"]["channelId"]
        channel_title = first_result["snippet"]["title"]
        logging.info(f"Found potential channel for '{institution_name}': '{channel_title}' (ID: {channel_id})")
        # Basic verification: Check if institution name is roughly in channel title
        if institution_name.lower() not in channel_title.lower():
             logging.warning(f"Potential mismatch: Institution '{institution_name}' vs Channel Title '{channel_title}'. Using it anyway.")
        return channel_id, channel_title, None

    except HttpError as e:
        logging.error(f"HTTP error searching for '{institution_name}': {e}")
        if e.resp.status == 403:
             logging.error("Quota likely exceeded. Stopping.")
             raise # Re-raise to stop the script
        return None, None, f"API Error: {e.resp.status}"
    except Exception as e:
        logging.error(f"Unexpected error searching for '{institution_name}': {e}")
        return None, None, f"Unexpected Error: {str(e)}"


# --- Main Execution ---
def main(institutions_file, output_csv):
    logging.info("--- Starting Channel Search Script ---")
    logging.info(f"Loading institutions from: {institutions_file}")
    logging.info(f"Outputting results to: {output_csv}")

    try:
        with open(institutions_file, 'r', encoding='utf-8') as f:
            institutions = [line.strip() for line in f if line.strip()]
        logging.info(f"Loaded {len(institutions)} institutions.")
    except FileNotFoundError:
        logging.error(f"Error: Institutions file not found at '{institutions_file}'")
        return
    except Exception as e:
        logging.error(f"Error reading institutions file: {e}")
        return

    youtube = initialize_youtube_api()
    results = []

    logging.info("Starting channel search loop...")
    for institution in institutions:
        if not institution: continue # Skip empty lines

        channel_id, channel_title, error_msg = search_channel(youtube, institution)
        results.append({
            "INSTITUTION": institution,
            "CHANNEL_ID": channel_id,
            "FOUND_CHANNEL_TITLE": channel_title,
            "ERROR_MESSAGE": error_msg
        })
        # Optional: Add a small delay to respect API quotas implicitly
        time.sleep(0.1)


    logging.info(f"Finished searching. Writing {len(results)} results to {output_csv}")
    try:
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["INSTITUTION", "CHANNEL_ID", "FOUND_CHANNEL_TITLE", "ERROR_MESSAGE"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(results)
        logging.info("Successfully wrote results to CSV.")
    except IOError as e:
        logging.error(f"Error writing results to CSV '{output_csv}': {e}")
    except Exception as e:
        logging.error(f"Unexpected error writing CSV: {e}")

    logging.info("--- Channel Search Script Finished ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search for YouTube channels for a list of institutions.")
    parser.add_argument(
        "--institutions",
        default=DEFAULT_INSTITUTIONS_FILE,
        help=f"Path to the text file containing institution names (one per line). Default: {DEFAULT_INSTITUTIONS_FILE}"
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_CSV,
        help=f"Path to the output CSV file. Default: {DEFAULT_OUTPUT_CSV}"
    )
    args = parser.parse_args()

    main(args.institutions, args.output) 