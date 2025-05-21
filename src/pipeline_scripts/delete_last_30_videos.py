import sqlite3
import os

DATABASE_NAME = "pipeline_database.db"
NUM_ROWS_TO_DELETE = 30

def delete_last_n_rows(db_file, num_rows):
    \"\"\"Deletes the last N rows from the videos table, based on the highest ID values.\"\"\"
    conn = None
    try:
        if not os.path.exists(db_file):
            print(f"Error: Database file '{db_file}' not found.")
            return

        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # First, get the IDs of the last N rows
        cursor.execute(f"SELECT id FROM videos ORDER BY id DESC LIMIT {num_rows}")
        rows_to_delete = cursor.fetchall()

        if not rows_to_delete:
            print("No rows found in the 'videos' table to delete.")
            return

        ids_to_delete = [row[0] for row in rows_to_delete]
        
        # Create a string of placeholders for the IN clause
        placeholders = ', '.join(['?'] * len(ids_to_delete))
        
        # Delete the rows with the selected IDs
        delete_sql = f"DELETE FROM videos WHERE id IN ({placeholders})"
        cursor.execute(delete_sql, ids_to_delete)
        
        deleted_count = cursor.rowcount
        conn.commit()
        
        print(f"Successfully deleted {deleted_count} rows from the 'videos' table.")

    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        print(f"SQLite error occurred: {e}")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print(f"Database connection to '{db_file}' closed.")

if __name__ == "__main__":
    print(f"Attempting to delete the last {NUM_ROWS_TO_DELETE} rows from '{DATABASE_NAME}'...")
    delete_last_n_rows(DATABASE_NAME, NUM_ROWS_TO_DELETE) 