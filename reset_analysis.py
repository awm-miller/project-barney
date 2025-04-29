import sqlite3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Default database path from environment variable
DATABASE_PATH = os.getenv("DATABASE_PATH")
if not DATABASE_PATH:
    raise ValueError("DATABASE_PATH not found in .env file. Please set it for the reset script.")

# Try to get ANALYSIS_DIR to delete the analysis map, but don't fail if it's not set
ANALYSIS_DIR = os.getenv("ANALYSIS_DIR")
DEFAULT_ANALYSIS_MAP_PATH = os.path.join(ANALYSIS_DIR, "analysis_map.json") if ANALYSIS_DIR else None

def reset_analysis_status(db_path=DATABASE_PATH, analysis_map_path=DEFAULT_ANALYSIS_MAP_PATH):
    """Reset all transcripts to be eligible for analysis again."""
    print(f"Connecting to database at: {db_path}")
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return
    
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get count before update
        cursor.execute("SELECT COUNT(*) FROM video_processing WHERE status IN ('analysis_complete', 'analysis_failed', 'analysis_pending')")
        count_before = cursor.fetchone()[0]
        print(f"Found {count_before} transcripts with analysis status")
        
        # Check if ai_analysis_content column exists
        cursor.execute("PRAGMA table_info(video_processing)")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Build update query based on available columns
        update_fields = ["status = 'transcription_complete'", "updated_at = CURRENT_TIMESTAMP"]
        
        if "ai_analysis_path" in columns:
            update_fields.append("ai_analysis_path = NULL")
        
        if "ai_analysis_content" in columns:
            update_fields.append("ai_analysis_content = NULL")
        
        if "error_message" in columns:
            update_fields.append("error_message = NULL")
        
        # Update all transcripts with existing analysis to transcription_complete status
        update_query = f"""
        UPDATE video_processing 
        SET {', '.join(update_fields)}
        WHERE status IN ('analysis_complete', 'analysis_failed', 'analysis_pending')
        """
        cursor.execute(update_query)
        
        # Commit the changes
        conn.commit()
        
        # Get count of transcripts ready for analysis
        cursor.execute("SELECT COUNT(*) FROM video_processing WHERE status = 'transcription_complete'")
        count_after = cursor.fetchone()[0]
        
        print(f"Successfully reset {count_before} transcripts to 'transcription_complete' status")
        print(f"Total transcripts now ready for analysis: {count_after}")
        
        # Delete analysis map file if path is known and it exists
        if analysis_map_path and os.path.exists(analysis_map_path):
            try:
                os.remove(analysis_map_path)
                print(f"Deleted analysis map file: {analysis_map_path}")
            except Exception as e:
                print(f"Error deleting analysis map file: {e}")
        elif analysis_map_path:
             print(f"Analysis map file not found at {analysis_map_path}, skipping deletion.")
        else:
            print("ANALYSIS_DIR environment variable not set, cannot determine analysis map path for deletion.")
        
        # Close connection
        conn.close()
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    reset_analysis_status() 