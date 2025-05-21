import sqlite3
import csv
import os
import argparse
from datetime import datetime

DEFAULT_DATABASE_NAME = "pipeline_database.db"
DEFAULT_CSV_FILENAME = "ai_analysis_export.csv"

def export_ai_content_to_csv(db_file, csv_file):
    conn = None
    try:
        if not os.path.exists(db_file):
            print(f"Error: Database file '{db_file}' not found.")
            return

        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Select the required columns, now including published_at and ordering by it
        cursor.execute("SELECT id, video_id, title, published_at, ai_analysis_content FROM videos ORDER BY published_at DESC")
        rows = cursor.fetchall()

        if not rows:
            print("No data found in the 'videos' table to export.")
            return

        headers = ["id", "video_id", "title", "published_at", "ai_analysis_content"]

        with open(csv_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(headers)  # Write the header row
            writer.writerows(rows)   # Write all data rows

        print(f"Successfully exported {len(rows)} rows to '{csv_file}'.")

    except sqlite3.Error as e:
        print(f"SQLite error occurred: {e}")
    except IOError as e:
        print(f"IOError occurred while writing to CSV: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print(f"Database connection to '{db_file}' closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export AI analysis content from a SQLite database to a CSV file.")
    parser.add_argument(
        "--db-name", 
        default=DEFAULT_DATABASE_NAME, 
        help=f"Name of the SQLite database file to use. Default: {DEFAULT_DATABASE_NAME}"
    )
    parser.add_argument(
        "--csv-file", 
        default=DEFAULT_CSV_FILENAME, 
        help=f"Name of the output CSV file. Default: {DEFAULT_CSV_FILENAME}"
    )
    args = parser.parse_args()

    print(f"Attempting to export AI analysis content from '{args.db_name}' to '{args.csv_file}'...")
    export_ai_content_to_csv(args.db_name, args.csv_file) 