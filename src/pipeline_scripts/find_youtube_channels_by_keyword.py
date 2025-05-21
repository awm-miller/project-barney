import os
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import argparse
import time
import sqlite3 # For sqlite3.Error

# Import from our database manager
from database_manager import create_connection, DATABASE_NAME

# --- Configuration ---
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    raise ValueError("YOUTUBE_API_KEY not found in .env file or environment variables.")

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

DEFAULT_INSTITUTIONS_FILE = "unique_institutions.csv"
# DEFAULT_OUTPUT_CSV = "search_results.csv" # Removed, will use database
LOG_FILE = "search_channels.log"
SCRIPT_NAME = "search_channels.py" # To record the source script

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# --- Database Helper Function ---
def add_or_update_channel_db(conn, institution_name: str, channel_id: str, channel_title: str, status: str, error_message: str):
    """
    Adds a new channel to the database or updates an existing one based on channel_id.
    Uses the channel_id as the conflict target.
    """
    sql = '''
    INSERT INTO channels (institution_name, channel_id, channel_title, source_script, status, error_message, added_at, last_updated_at)
    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT(channel_id) DO UPDATE SET
        institution_name = excluded.institution_name,
        channel_title = excluded.channel_title,
        source_script = excluded.source_script,
        status = excluded.status,
        error_message = excluded.error_message,
        last_updated_at = CURRENT_TIMESTAMP;
    '''
    # If channel_id is None (search failed for institution without finding a distinct channel),
    # we can't use ON CONFLICT(channel_id). We'll insert with a placeholder or handle differently.
    # For now, we'll assume search_channel tries to return *some* channel_id if one is picked,
    # or None if truly nothing is found or an API error specific to the search occurs.

    # If channel_id is None, it means the search for this institution failed to identify a specific channel.
    # We still want to record the attempt for the institution.
    # However, the 'channels' table has a UNIQUE constraint on channel_id.
    # We need a way to log institutions where no channel was found.
    # Option 1: Modify DB schema to allow NULL channel_id but unique (institution_name, source_script)
    # Option 2: Log to a different table or handle these 'no channel found' cases differently.
    # For now, if channel_id is None, we can't use the ON CONFLICT(channel_id).
    # Let's log a failed search for an institution by inserting if channel_id is present,
    # and handling the 'no specific channel found for institution' case.

    # If a channel_id is found, we insert/update.
    if channel_id:
        try:
            cursor = conn.cursor()
            cursor.execute(sql, (institution_name, channel_id, channel_title, SCRIPT_NAME, status, error_message))
            conn.commit()
            logging.info(f"Successfully recorded channel for '{institution_name}' (ID: {channel_id}) in database with status '{status}'.")
        except sqlite3.Error as e:
            logging.error(f"Database error while recording channel for '{institution_name}' (ID: {channel_id}): {e}")
            logging.error(f"SQL: {sql}")
            logging.error(f"Params: {(institution_name, channel_id, channel_title, SCRIPT_NAME, status, error_message)}")
    else:
        # Case: No specific channel_id returned by search_channel for this institution.
        # We should record that a search was attempted for the institution.
        # The current `channels` table requires a UNIQUE channel_id.
        # This case needs a more robust solution, potentially a separate log or schema adjustment.
        # For this iteration, we will log it and skip DB insertion for "no channel_id found".
        # A better approach might be to update an 'institutions_search_log' table.
        logging.warning(f"No specific channel_id found for institution '{institution_name}'. Status: '{status}', Error: '{error_message}'. Not inserting into 'channels' table as it requires a channel_id.")
        # To properly record this, we'd typically do:
        # try:
        #     cursor = conn.cursor()
        #     # Example: Insert into a hypothetical 'institution_search_attempts' table
        #     # cursor.execute("INSERT INTO institution_search_attempts (institution_name, status, error_message, source_script) VALUES (?, ?, ?, ?)",
        #     #                (institution_name, status, error_message, SCRIPT_NAME))
        #     # conn.commit()
        #     # logging.info(f"Logged search attempt for institution '{institution_name}' with status '{status}'.")
        # except sqlite3.Error as e:
        #     logging.error(f"Database error while logging search attempt for '{institution_name}': {e}")


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
        return channel_id, channel_title, None # error_msg is None if successful

    except HttpError as e:
        logging.error(f"HTTP error searching for '{institution_name}': {e}")
        if e.resp.status == 403:
             logging.error("Quota likely exceeded. Stopping.")
             raise # Re-raise to stop the script
        return None, None, f"API Error: {e.resp.status}" # Return error message
    except Exception as e:
        logging.error(f"Unexpected error searching for '{institution_name}': {e}")
        return None, None, f"Unexpected Error: {str(e)}" # Return error message


# --- Main Execution ---
def main(institutions_file): # Removed output_csv
    logging.info("--- Starting Channel Search Script ---")
    logging.info(f"Loading institutions from: {institutions_file}")
    # logging.info(f"Outputting results to: {output_csv}") # Removed

    conn = create_connection(DATABASE_NAME)
    if not conn:
        logging.error("Could not connect to the database. Exiting.")
        return

    try:
        with open(institutions_file, 'r', encoding='utf-8') as f:
            institutions = [line.strip() for line in f if line.strip()]
        logging.info(f"Loaded {len(institutions)} institutions.")
    except FileNotFoundError:
        logging.error(f"Error: Institutions file not found at '{institutions_file}'")
        if conn: conn.close()
        return
    except Exception as e:
        logging.error(f"Error reading institutions file: {e}")
        if conn: conn.close()
        return

    youtube = initialize_youtube_api()
    # results = [] # Removed, will write directly to DB

    logging.info("Starting channel search loop...")
    total_institutions = len(institutions)
    processed_count = 0
    found_count = 0
    failed_search_count = 0
    api_error_count = 0

    try:
        for i, institution in enumerate(institutions):
            if not institution: continue # Skip empty lines
            logging.info(f"Processing institution [{i+1}/{total_institutions}]: {institution}")

            channel_id, channel_title, error_msg = search_channel(youtube, institution)
            
            status = ""
            if channel_id:
                status = "found"
                found_count +=1
            elif error_msg and "API Error" in error_msg:
                status = "api_error"
                api_error_count += 1
            else: # Includes "No channel found" or other unexpected errors
                status = "search_failed"
                failed_search_count += 1
            
            # Record the result in the database
            # Note: The current add_or_update_channel_db only inserts if channel_id is not None.
            # This means institutions for which no channel_id is found (status='search_failed' or 'api_error' with no channel_id)
            # won't be added to the 'channels' table by that function as is.
            # This is a limitation noted in the add_or_update_channel_db comments.
            # For true logging of all attempts, schema/logic adjustment is needed.
            add_or_update_channel_db(conn, institution, channel_id, channel_title, status, error_msg)

            processed_count += 1
            # Optional: Add a small delay to respect API quotas implicitly
            time.sleep(0.1) # Keep this small delay

    except HttpError as e: # Catch quota errors if search_channel re-raises
        logging.critical(f"A critical YouTube API HttpError occurred (likely quota): {e}. Stopping script.")
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred: {e}. Stopping script.")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    logging.info("--- Channel Search Script Finished ---")
    logging.info(f"Total institutions processed: {processed_count}")
    logging.info(f"  Channels found and recorded: {found_count}")
    logging.info(f"  Searches failed (no channel identified): {failed_search_count}")
    logging.info(f"  API errors during search: {api_error_count}")
    # The add_or_update_channel_db logs skipped insertions if channel_id is None.
    # Consider if results for institutions where no channel_id was found should be stored elsewhere.


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search for YouTube channels for a list of institutions and records them in a database.")
    parser.add_argument(
        "--institutions",
        default=DEFAULT_INSTITUTIONS_FILE,
        help=f"Path to the text file containing institution names (one per line). Default: {DEFAULT_INSTITUTIONS_FILE}"
    )
    # parser.add_argument(
    #     "--output",
    #     default=DEFAULT_OUTPUT_CSV,
    #     help=f"Path to the output CSV file. Default: {DEFAULT_OUTPUT_CSV}"
    # ) # Removed output argument
    args = parser.parse_args()

    main(args.institutions) 