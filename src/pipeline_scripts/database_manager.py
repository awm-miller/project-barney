import sqlite3
from sqlite3 import Error
import argparse
from typing import Optional

DATABASE_NAME = "pipeline_database.db"
DEFAULT_DB_NAME = DATABASE_NAME # For broader use

def create_connection(db_file=DEFAULT_DB_NAME):
    """ Create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Successfully connected to SQLite database: {db_file} (SQLite version: {sqlite3.sqlite_version})")
    except Error as e:
        print(f"Error connecting to database: {e}")
    return conn

def create_videos_table(conn):
    """ Create the videos table with the specified schema """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        video_id TEXT UNIQUE NOT NULL,
        video_url TEXT NOT NULL,
        channel_id TEXT,
        title TEXT,
        status TEXT DEFAULT 'NEW',         -- General status for new video entries
        source_script TEXT,               -- Script that added this video (e.g., search_script.py)
        published_at TIMESTAMP,           -- Original publication date of the video
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the video was added to our DB
        
        -- Subtitle Fetch Phase (NEW)
        subtitle_status TEXT DEFAULT 'pending_check', -- e.g., pending_check, available, unavailable, fetched, error
        subtitle_fetched_at TIMESTAMP,
        subtitle_file_path TEXT,
        subtitle_error_message TEXT,
        text_source TEXT,                           -- To indicate origin: SUBTITLE, TRANSCRIPTION

        -- Subtitle to Plain Text Conversion Phase (NEW)
        plain_text_subtitle_path TEXT,
        subtitle_to_text_status TEXT DEFAULT 'pending',
        subtitle_to_text_initiated_at TIMESTAMP,
        subtitle_to_text_completed_at TIMESTAMP,
        subtitle_to_text_error_message TEXT,

        -- Download Phase
        download_status TEXT DEFAULT 'pending',
        download_initiated_at TIMESTAMP,
        download_completed_at TIMESTAMP,
        download_path TEXT,
        download_error_message TEXT,
        
        -- Word-level Transcription Phase
        transcription_status TEXT DEFAULT 'pending',
        transcription_initiated_at TIMESTAMP,
        transcription_completed_at TIMESTAMP,
        transcription_path TEXT,            -- Path to the word-level transcript
        transcription_error_message TEXT,
        gcs_blob_name TEXT,                 -- GCS blob name for the audio file used in transcription
        gcp_operation_name TEXT,            -- GCP operation name for long-running transcription

        -- 10-Word Segmentation Phase (NEW)
        segmented_10w_transcript_path TEXT, -- Path to the 10-word segmented transcript
        segmentation_10w_status TEXT DEFAULT 'pending',
        segmentation_10w_error_message TEXT, -- Corrected column name
        segmentation_10w_initiated_at TIMESTAMP,
        segmentation_10w_completed_at TIMESTAMP,

        -- Analysis Phase (Retained for now, can be repurposed or removed later if not used)
        analysis_status TEXT DEFAULT 'pending',
        analysis_initiated_at TIMESTAMP,
        analysis_completed_at TIMESTAMP,
        analysis_error_message TEXT,
        ai_analysis_path TEXT,
        ai_analysis_content TEXT,
        description TEXT,
        last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        print("Table 'videos' created successfully (or already exists).")

        # --- Add columns if they don't exist (for existing databases) ---
        columns_to_add = [
            ("status", "TEXT DEFAULT 'NEW'"),
            ("published_at", "TIMESTAMP"),
            # New segmentation columns
            ("segmented_10w_transcript_path", "TEXT"),
            ("segmentation_10w_status", "TEXT DEFAULT 'pending'"),
            ("segmentation_10w_error_message", "TEXT"),
            ("segmentation_10w_initiated_at", "TIMESTAMP"),
            ("segmentation_10w_completed_at", "TIMESTAMP"),
            # New subtitle columns
            ("subtitle_status", "TEXT DEFAULT 'pending_check'"),
            ("subtitle_fetched_at", "TIMESTAMP"),
            ("subtitle_file_path", "TEXT"),
            ("subtitle_error_message", "TEXT"),
            ("text_source", "TEXT"),
            # New subtitle to text conversion columns
            ("plain_text_subtitle_path", "TEXT"),
            ("subtitle_to_text_status", "TEXT DEFAULT 'pending'"),
            ("subtitle_to_text_initiated_at", "TIMESTAMP"),
            ("subtitle_to_text_completed_at", "TIMESTAMP"),
            ("subtitle_to_text_error_message", "TEXT"),
            ("description", "TEXT")
        ]

        for col_name, col_type in columns_to_add:
            try:
                alter_table_sql = f"ALTER TABLE videos ADD COLUMN {col_name} {col_type}"
                cursor.execute(alter_table_sql)
                print(f"Column '{col_name}' added to 'videos' table.")
            except sqlite3.OperationalError as e:
                if f"duplicate column name: {col_name}" in str(e):
                    print(f"Column '{col_name}' already exists in 'videos' table.")
                else:
                    print(f"Unexpected OperationalError when adding column {col_name}: {e}")
                    # Decide if you want to raise e here or just log and continue
                    # For schema evolution, it might be better to log and attempt to continue
                    # raise e 

        # Add a trigger to update last_updated_at on any row update
        trigger_sql = """
        CREATE TRIGGER IF NOT EXISTS update_last_updated_at
        AFTER UPDATE ON videos
        FOR EACH ROW
        BEGIN
            UPDATE videos SET last_updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
        END;
        """
        cursor.execute(trigger_sql)
        print("Trigger 'update_last_updated_at' created successfully (or already exists).")
        conn.commit()
    except Error as e:
        print(f"Error creating table or trigger: {e}")

def create_channels_table(conn):
    """ Create the channels table """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        institution_name TEXT,
        channel_id TEXT UNIQUE NOT NULL,
        channel_title TEXT,
        source_script TEXT,
        status TEXT NOT NULL DEFAULT 'pending_search',
        error_message TEXT,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_searched_for_videos_at TIMESTAMP,
        last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    create_trigger_sql = """
    CREATE TRIGGER IF NOT EXISTS update_channels_last_updated_at
    AFTER UPDATE ON channels
    FOR EACH ROW
    BEGIN
        UPDATE channels SET last_updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
    END;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        print("Table 'channels' created successfully (or already exists).")
        cursor.execute(create_trigger_sql)
        print("Trigger 'update_channels_last_updated_at' created successfully (or already exists).")
        conn.commit()
    except Error as e:
        print(f"Error creating 'channels' table or trigger: {e}")

def create_processing_logs_table(conn):
    """ Create the processing_logs table """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS processing_logs (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        video_record_id INTEGER,
        stage TEXT,
        status TEXT,
        message TEXT,
        details TEXT,
        source_script TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (video_record_id) REFERENCES videos (id)
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        print("Table 'processing_logs' created successfully (or already exists).")
        
        # Attempt to add the source_script column if it doesn't exist
        try:
            alter_table_sql = "ALTER TABLE processing_logs ADD COLUMN source_script TEXT"
            cursor.execute(alter_table_sql)
            print("Column 'source_script' added to 'processing_logs' table.")
        except sqlite3.OperationalError as e:
            if "duplicate column name: source_script" in str(e):
                print("Column 'source_script' already exists in 'processing_logs' table.")
            else:
                print(f"Unexpected OperationalError when adding source_script to processing_logs: {e}")
                raise # Re-raise other operational errors
        
        conn.commit()
    except Error as e:
        print(f"Error creating 'processing_logs' table: {e}")

def reset_video_download_statuses(conn):
    """ Resets download-related fields for all videos in the database. """
    sql_update_videos = """
    UPDATE videos
    SET download_status = 'pending',
        download_initiated_at = NULL,
        download_completed_at = NULL,
        download_path = NULL,
        download_error_message = NULL,
        last_updated_at = CURRENT_TIMESTAMP;
    """
    # Optionally, you might want to clear related processing_logs for 'download' stage
    # sql_delete_download_logs = "DELETE FROM processing_logs WHERE stage = 'download';"
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql_update_videos)
        updated_rows = cursor.rowcount
        print(f"Reset download status for {updated_rows} videos.")
        
        # Uncomment to delete old download logs
        # cursor.execute(sql_delete_download_logs)
        # deleted_logs = cursor.rowcount
        # print(f"Deleted {deleted_logs} 'download' stage entries from processing_logs.")
        
        conn.commit()
        print("Video download statuses reset successfully.")
    except Error as e:
        print(f"Error resetting video download statuses: {e}")

def delete_videos_before_date(conn, date_string):
    """ Deletes video entries from the 'videos' table published before a given date. """
    sql_delete = "DELETE FROM videos WHERE published_at <= ?"
    try:
        cursor = conn.cursor()
        cursor.execute(sql_delete, (date_string,))
        deleted_rows = cursor.rowcount
        conn.commit()
        print(f"Successfully deleted {deleted_rows} videos published before {date_string}.")
    except Error as e:
        print(f"Error deleting old videos: {e}")
        conn.rollback() # Rollback changes in case of error

def create_published_at_index(conn):
    """Create an index on the published_at column for faster sorting."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_videos_published_at 
        ON videos(published_at DESC)
        """)
        conn.commit()
        print("Index on published_at column created successfully (or already exists).")
    except Error as e:
        print(f"Error creating index: {e}")

def reset_stuck_downloads(conn):
    """ Resets the status of videos stuck in 'downloading' back to 'pending'. """
    sql_update = """
    UPDATE videos
    SET download_status = 'pending',
        download_initiated_at = NULL,
        download_completed_at = NULL,
        download_path = NULL,
        download_error_message = NULL,
        last_updated_at = CURRENT_TIMESTAMP
    WHERE download_status = 'downloading';
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql_update)
        reset_count = cursor.rowcount
        conn.commit()
        print(f"Reset {reset_count} videos from 'downloading' back to 'pending'.")
    except Error as e:
        print(f"Error resetting stuck downloads: {e}")
        conn.rollback()

def reset_all_transcriptions(conn):
    """ Resets transcription status and related fields for all videos. """
    sql_update = """
    UPDATE videos
    SET transcription_status = 'pending',
        transcription_path = NULL,
        gcs_blob_name = NULL,
        gcp_operation_name = NULL,
        transcription_error_message = NULL,
        transcription_initiated_at = NULL,
        transcription_completed_at = NULL,
        
        -- Also reset 10-word segmentation fields
        segmentation_10w_status = 'pending',
        segmented_10w_transcript_path = NULL,
        segmentation_10w_error_message = NULL,
        segmentation_10w_initiated_at = NULL,
        segmentation_10w_completed_at = NULL,

        -- Reset subtitle fields as well, or handle separately?
        -- For now, let's assume a full transcription reset implies re-checking everything.
        -- However, a more granular reset for subtitles might be needed later.
        subtitle_status = 'pending_check', 
        subtitle_file_path = NULL,
        subtitle_error_message = NULL,
        subtitle_fetched_at = NULL,
        text_source = NULL, -- Resetting text_source if transcription is reset
        
        -- Reset subtitle to text fields (NEW)
        plain_text_subtitle_path = NULL,
        subtitle_to_text_status = 'pending',
        subtitle_to_text_initiated_at = NULL,
        subtitle_to_text_completed_at = NULL,
        subtitle_to_text_error_message = NULL,

        last_updated_at = CURRENT_TIMESTAMP;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql_update)
        reset_count = cursor.rowcount
        conn.commit()
        print(f"Reset transcription status for {reset_count} videos to 'pending'.")
    except Error as e:
        print(f"Error resetting transcriptions: {e}")
        conn.rollback()

def get_videos_for_10w_segmentation(conn, limit=None, job_name: Optional[str] = None):
    """Fetches videos that have completed word-level transcription and are pending 10w segmentation
       AND are from a transcription source, not subtitles.
    """
    cursor = conn.cursor()
    table_name = f"videos_{job_name}" if job_name else "videos"

    sql = f"""
    SELECT id, transcription_path, title, video_id
    FROM {table_name}
    WHERE transcription_status = 'completed' 
      AND segmentation_10w_status = 'pending'
      AND (text_source = 'TRANSCRIPTION' OR text_source IS NULL) -- Only process GCS transcriptions
    ORDER BY transcription_completed_at ASC -- Process older successful transcriptions first
    """
    params = []
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    
    try:
        cursor.execute(sql, params)
        videos = cursor.fetchall()
        video_list = [dict(zip([column[0] for column in cursor.description], row)) for row in videos]
        print(f"Found {len(video_list)} videos from GCS transcription source for 10-word segmentation in table '{table_name}'.")
        return video_list
    except sqlite3.Error as e:
        print(f"Database error fetching videos for 10w segmentation: {e}")
        return []

def update_video_segmentation_10w_status(conn, video_db_id: int, status: str, 
                                        segmented_transcript_path: Optional[str] = None, 
                                        error_message: Optional[str] = None,
                                        initiated: bool = False, completed: bool = False):
    """Updates the 10-word segmentation status and related fields for a video."""
    cursor = conn.cursor()
    set_clauses = ["segmentation_10w_status = ?"]
    parameters = [status]

    if segmented_transcript_path is not None:
        set_clauses.append("segmented_10w_transcript_path = ?")
        parameters.append(segmented_transcript_path)
    
    if error_message is not None:
        set_clauses.append("segmentation_10w_error_message = ?")
        parameters.append(error_message)
    elif status == 'completed' or status == 'pending': # Clear error on success or reset
        set_clauses.append("segmentation_10w_error_message = NULL")

    if initiated:
        set_clauses.append("segmentation_10w_initiated_at = CURRENT_TIMESTAMP")
        set_clauses.append("segmentation_10w_completed_at = NULL")
        if status != 'failed':
            set_clauses.append("segmentation_10w_error_message = NULL")
    elif completed:
        set_clauses.append("segmentation_10w_completed_at = CURRENT_TIMESTAMP")
        if status == 'completed': # Ensure error is cleared on successful completion
             set_clauses.append("segmentation_10w_error_message = NULL")

    set_clauses.append("last_updated_at = CURRENT_TIMESTAMP")
    
    sql = f"UPDATE videos SET {', '.join(set_clauses)} WHERE id = ?"
    parameters.append(video_db_id)
    
    try:
        cursor.execute(sql, parameters)
        conn.commit()
        print(f"Updated video (DB ID: {video_db_id}) segmentation_10w_status to: '{status}'.")
    except sqlite3.Error as e:
        conn.rollback()
        print(f"Database error updating video (DB ID: {video_db_id}) 10w segmentation status: {e}")

def reset_videos_for_reprocessing(conn):
    """ Resets videos to a state where they can be reprocessed from scratch,
        keeping essential identifiers like video_id, channel_id, title, video_url,
        and published_at.
    """
    # video_id, video_url, channel_id, title, published_at, added_at are preserved.
    sql_update_videos_refined = """
    UPDATE videos
    SET
        status = 'NEW',
        source_script = NULL,
        subtitle_status = 'pending_check',
        subtitle_fetched_at = NULL,
        subtitle_file_path = NULL,
        subtitle_error_message = NULL,
        text_source = NULL,
        plain_text_subtitle_path = NULL,
        subtitle_to_text_status = 'pending',
        subtitle_to_text_initiated_at = NULL,
        subtitle_to_text_completed_at = NULL,
        subtitle_to_text_error_message = NULL,
        download_status = 'pending',
        download_initiated_at = NULL,
        download_completed_at = NULL,
        download_path = NULL,
        download_error_message = NULL,
        transcription_status = 'pending',
        transcription_initiated_at = NULL,
        transcription_completed_at = NULL,
        transcription_path = NULL,
        transcription_error_message = NULL,
        gcs_blob_name = NULL,
        gcp_operation_name = NULL,
        segmented_10w_transcript_path = NULL,
        segmentation_10w_status = 'pending',
        segmentation_10w_error_message = NULL,
        segmentation_10w_initiated_at = NULL,
        segmentation_10w_completed_at = NULL,
        analysis_status = 'pending',
        analysis_initiated_at = NULL,
        analysis_completed_at = NULL,
        analysis_error_message = NULL,
        ai_analysis_path = NULL,
        ai_analysis_content = NULL,
        last_updated_at = CURRENT_TIMESTAMP;
    """

    try:
        cursor = conn.cursor()
        cursor.execute(sql_update_videos_refined)
        updated_rows = cursor.rowcount
        conn.commit()
        print(f"Successfully reset {updated_rows} videos for reprocessing.")
    except Error as e:
        print(f"Error resetting videos for reprocessing: {e}")
        conn.rollback()

def initialize_database(db_to_init=DEFAULT_DB_NAME):
    """Initialize the database with all required tables and indexes."""
    # Use the provided db_name or default if called directly
    conn = create_connection(db_to_init)
    if conn is not None:
        create_videos_table(conn)
        create_channels_table(conn)
        create_processing_logs_table(conn)
        create_published_at_index(conn)
        conn.close()
    else:
        print("Error! Cannot create the database connection.")

def reset_summarization_status(conn):
    """ Resets summarization-related fields for all videos in the database. """
    sql_update_videos = """
    UPDATE videos
    SET analysis_status = 'pending',
        analysis_initiated_at = NULL,
        analysis_completed_at = NULL,
        analysis_error_message = NULL,
        ai_analysis_content = NULL,
        ai_analysis_path = NULL,
        last_updated_at = CURRENT_TIMESTAMP;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql_update_videos)
        updated_rows = cursor.rowcount
        print(f"Reset summarization status for {updated_rows} videos.")
        conn.commit()
        print("Video summarization statuses reset successfully.")
    except Error as e:
        print(f"Error resetting video summarization statuses: {e}")
        conn.rollback()

def main():
    parser = argparse.ArgumentParser(description="Manage the SQLite database for the pipeline.")
    parser.add_argument("--initialize", action="store_true",
                        help="Initialize the database with all tables and indexes.")
    parser.add_argument("--reset-stuck-downloads", action="store_true",
                        help="Reset download status for videos stuck in 'downloading' for more than an hour.")
    parser.add_argument("--reset-transcriptions", action="store_true",
                        help="Reset all transcription statuses to 'pending'.")
    parser.add_argument("--delete-before-date", type=str, help="Delete videos published before this date (YYYY-MM-DD).")
    parser.add_argument("--reset-downloads", action="store_true", help="Reset download status for all videos.")
    parser.add_argument("--reinitialize-soft", action="store_true", help="Resets all video processing statuses, keeping video_id, channel_id etc., to allow reprocessing.")
    parser.add_argument("--reset-summarization", action="store_true", help="Reset AI summarization status for all videos.")
    parser.add_argument(
        "--db-name", 
        default=DEFAULT_DB_NAME, 
        help=f"Name of the SQLite database file to use for operations. Default: {DEFAULT_DB_NAME}"
    )

    args = parser.parse_args()
    
    # Use the db_name from args for creating the connection for operations
    print(f"Operating on database: {args.db_name}")
    connection = create_connection(args.db_name)

    if connection is not None:
        action_taken = False
        if args.initialize:
            print("Initializing database...")
            initialize_database(args.db_name) # Manages its own connection internally
            action_taken = True
        
        if args.reset_stuck_downloads: # Changed to 'if' to allow multiple reset types if desired, or keep as 'elif' if mutually exclusive
            print("Resetting stuck downloads...")
            reset_stuck_downloads(connection)
            action_taken = True
        
        if args.reset_transcriptions: # Changed to 'if'
            print("Resetting all transcriptions to 'pending'...")
            reset_all_transcriptions(connection)
            action_taken = True
        
        if args.delete_before_date: # Changed to 'if'
            delete_videos_before_date(connection, args.delete_before_date)
            action_taken = True
            
        if args.reset_downloads: # Changed to 'if'
            reset_video_download_statuses(connection) # Assumes this is the correct function for the generic --reset-downloads
            action_taken = True

        if args.reinitialize_soft: # Changed to 'if'
            print("Resetting all video processing statuses for reprocessing (soft reset)...")
            reset_videos_for_reprocessing(connection)
            action_taken = True

        if args.reset_summarization:
            print("Resetting AI summarization statuses...")
            reset_summarization_status(connection)
            action_taken = True

        if not action_taken:
            # Default behavior: standard setup/check (which `initialize_database` implicitly does via `CREATE TABLE IF NOT EXISTS`)
            print("No specific action requested. Performing standard database setup/check...")
            initialize_database(args.db_name) # Ensures tables are checked/created if no other action. Manages its own connection.
                                  # If initialize_database() is meant to use the 'connection' object, it should be:
                                  # create_videos_table(connection)
                                  # create_channels_table(connection)
                                  # create_processing_logs_table(connection)
                                  # etc.
                                  # The previous `read_file` showed `initialize_database` does not take args,
                                  # implying it calls `create_connection` itself.

        if connection: # Connection object from this scope, not the one initialize_database might create and close.
            connection.close()
            print("Database connection (main) closed.")
    else:
        print("Failed to create database connection. No actions performed.")

if __name__ == '__main__':
    main() 