#!/usr/bin/env python3

import sqlite3
from datetime import datetime
from database_manager import create_connection, DATABASE_NAME

def get_videos_sorted_by_date(conn):
    """Get all videos sorted by published_at in descending order."""
    cursor = conn.cursor()
    sql = """
    SELECT 
        v.id,
        v.video_id,
        v.title,
        v.published_at,
        v.download_status,
        c.channel_title,
        c.institution_name
    FROM videos v
    LEFT JOIN channels c ON v.channel_id = c.channel_id
    ORDER BY v.published_at DESC
    """
    
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []

def format_date(date_str):
    """Format the date string to be more readable."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return date_str

def main():
    print("--- Videos Sorted by Publication Date (Most Recent First) ---")
    
    conn = create_connection(DATABASE_NAME)
    if not conn:
        print("Could not connect to the database. Exiting.")
        return

    try:
        videos = get_videos_sorted_by_date(conn)
        if not videos:
            print("No videos found in the database.")
            return

        print(f"\nFound {len(videos)} videos:")
        print("\n{:<5} {:<15} {:<50} {:<20} {:<15} {:<30} {:<30}".format(
            "ID", "Video ID", "Title", "Published At", "Status", "Channel", "Institution"
        ))
        print("-" * 160)

        for video in videos:
            db_id, video_id, title, published_at, status, channel, institution = video
            # Truncate long strings
            title = (title[:47] + "...") if title and len(title) > 50 else title
            channel = (channel[:27] + "...") if channel and len(channel) > 30 else channel
            institution = (institution[:27] + "...") if institution and len(institution) > 30 else institution
            
            print("{:<5} {:<15} {:<50} {:<20} {:<15} {:<30} {:<30}".format(
                db_id,
                video_id,
                title or "N/A",
                format_date(published_at) if published_at else "N/A",
                status or "N/A",
                channel or "N/A",
                institution or "N/A"
            ))

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 