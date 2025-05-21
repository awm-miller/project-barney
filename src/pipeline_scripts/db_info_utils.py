import sqlite3
import os

def get_database_summary(db_path):
    """Connects to an SQLite database and returns summary information."""
    summary = {
        "video_count": "N/A",
        "status": "N/A", # Overall status
        "error": None
    }

    if not os.path.exists(db_path):
        summary["error"] = "Database file not found."
        summary["status"] = "Error: Not Found"
        return summary

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1. Get video count (assuming a table named 'videos')
        try:
            cursor.execute("SELECT COUNT(*) FROM videos") # Replace 'videos' with your actual table name
            count_result = cursor.fetchone()
            if count_result:
                summary["video_count"] = count_result[0]
        except sqlite3.Error as e:
            # Table might not exist, or other SQL error
            summary["video_count"] = "Error fetching" 
            # Optionally log e or set a more specific error status for video count

        # 2. Determine overall status (this is highly dependent on your schema)
        # Example: Check for presence of key tables or columns, or specific status fields
        # For now, a placeholder based on successful connection and video count query:
        if summary["video_count"] != "Error fetching" and summary["video_count"] != "N/A":
            summary["status"] = "Ready" # Or more detailed based on your logic
        elif summary["video_count"] == "Error fetching":
            summary["status"] = "Schema Error?"
        else:
            summary["status"] = "Needs Check"
        
        # Add more specific status checks based on your pipeline stages:
        # For example, if you have a 'subtitles_fetched' column in your 'videos' table:
        # cursor.execute("SELECT COUNT(*) FROM videos WHERE subtitles_fetched = 1")
        # fetched_count = cursor.fetchone()[0]
        # cursor.execute("SELECT COUNT(*) FROM videos WHERE ai_analysis_done = 1")
        # analyzed_count = cursor.fetchone()[0]
        # summary["detailed_status"] = f"Subs Fetched: {fetched_count}/{summary['video_count']}, Analyzed: {analyzed_count}/{summary['video_count']}"

        conn.close()

    except sqlite3.Error as e:
        summary["error"] = f"SQLite error: {e}"
        summary["status"] = "Error: DB Connection"
    except Exception as e:
        summary["error"] = f"An unexpected error occurred: {e}"
        summary["status"] = "Error: Unexpected"
        
    return summary

if __name__ == '__main__':
    # Example usage (for testing this script directly)
    # Create a dummy DB for testing if you don't have one readily available
    # For example, in your project root/databases folder:
    # dummy_db_path = os.path.join("..", "databases", "test_db.db") 
    # if not os.path.exists(dummy_db_path):
    #     conn_test = sqlite3.connect(dummy_db_path)
    #     cursor_test = conn_test.cursor()
    #     try:
    #         cursor_test.execute("CREATE TABLE IF NOT EXISTS videos (id INTEGER PRIMARY KEY, title TEXT)")
    #         cursor_test.execute("INSERT INTO videos (title) VALUES ('Test Video 1')")
    #         cursor_test.execute("INSERT INTO videos (title) VALUES ('Test Video 2')")
    #         conn_test.commit()
    #     except sqlite3.Error as e_test:
    #         print(f"Error creating dummy db: {e_test}")
    #     finally:
    #         conn_test.close()
    
    # print(f"Testing with: {dummy_db_path}")
    # summary = get_database_summary(dummy_db_path)
    # print(summary)

    # print(get_database_summary(os.path.join("..", "databases", "london_dialogues.db"))) # Replace with an actual DB name
    # print(get_database_summary(os.path.join("..", "databases", "reflections.db"))) # Replace with an actual DB name
    pass 