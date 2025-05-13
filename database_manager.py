import sqlite3
from sqlite3 import Error
import argparse
from typing import Optional

DATABASE_NAME = "pipeline_database.db"

def create_connection(db_file=DATABASE_NAME):
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
            ("segmentation_10w_completed_at", "TIMESTAMP")
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

def get_videos_for_10w_segmentation(conn, limit=None):
    """Fetches videos that have completed word-level transcription and are pending 10w segmentation."""
    cursor = conn.cursor()
    sql = """
    SELECT id, transcription_path, title
    FROM videos
    WHERE transcription_status = 'completed' 
      AND segmentation_10w_status = 'pending'
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
        print(f"Found {len(video_list)} videos for 10-word segmentation.")
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

def initialize_database():
    """Initialize the database with all required tables and indexes."""
    conn = create_connection(DATABASE_NAME)
    if conn is not None:
        create_videos_table(conn)
        create_channels_table(conn)
        create_processing_logs_table(conn)
        create_published_at_index(conn)
        conn.close()
    else:
        print("Error! Cannot create the database connection.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Manage the video pipeline database.")
    parser.add_argument("--reset-transcriptions", action="store_true",
                        help="Reset transcription status for all videos to 'pending'.")
    # Add other arguments here if needed in the future, e.g., for other reset operations
    parser.add_argument("--initialize", action="store_true",
                        help="Initialize the database with all tables and indexes.")

    args = parser.parse_args()

    connection = create_connection()
    if connection:
        if args.initialize:
            print("Initializing full database...")
            initialize_database() # Call the comprehensive initialization function
            print("Full database initialization complete.")
        elif args.reset_transcriptions:
            print("Resetting all video transcription statuses to 'pending'...")
            reset_all_transcriptions(connection)
            # The reset_all_transcriptions function already prints its own success/error message
        else:
            # Default behavior if no specific action is requested: standard setup/check
            print("Performing standard database setup/check (tables will be created if they don't exist)...")
            create_videos_table(connection)
            create_channels_table(connection)
            create_processing_logs_table(connection)
            create_published_at_index(connection) # Ensure index is also part of standard check
            print("Standard database setup/check complete.")

        connection.close()
        print("Database connection closed.")
    else:
        print("Failed to create database connection. No actions performed.") 