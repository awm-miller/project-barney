import sqlite3
from pathlib import Path
from typing import List, Dict, Optional
from src.config import KNOWN_DATABASES_FILE, DATABASES_DIR, LAST_OPENED_DB_FILE

def load_known_databases() -> List[Path]:
    """Loads database paths from known_databases.txt and scans DATABASES_DIR."""
    known_paths = set()
    if KNOWN_DATABASES_FILE.exists():
        with open(KNOWN_DATABASES_FILE, "r") as f:
            for line in f:
                path_str = line.strip()
                if path_str:  # Ensure not an empty line
                    known_paths.add(Path(path_str).resolve())

    if DATABASES_DIR.exists():
        for db_file in DATABASES_DIR.glob("*.db"):
            known_paths.add(db_file.resolve())

    unique_resolved_paths = sorted(list(known_paths))
    with open(KNOWN_DATABASES_FILE, "w") as f:
        for path in unique_resolved_paths:
            f.write(str(path) + "\n")
            
    return unique_resolved_paths

def add_known_database(db_path: Path, page=None): # Keep page optional for now
    """Adds a database path to known_databases.txt and updates the view if page is provided."""
    db_path = db_path.resolve()
    current_paths = load_known_databases()
    if db_path not in current_paths:
        with open(KNOWN_DATABASES_FILE, "a") as f:
            f.write(str(db_path) + "\n")
    if page and hasattr(page, 'selected_db_changed_callback'):
        page.selected_db_changed_callback()

def save_last_opened_db(db_path: Optional[Path]): # Allow db_path to be None
    """Saves the path of the last opened database. Clears the file if db_path is None."""
    try:
        with open(LAST_OPENED_DB_FILE, "w") as f:
            if db_path:
                f.write(str(db_path.resolve()))
            else:
                f.write("") # Write an empty string to clear the file content
    except Exception as e:
        print(f"Error saving last opened database: {e}")

def load_last_opened_db() -> Optional[Path]:
    """Loads the path of the last opened database."""
    try:
        if LAST_OPENED_DB_FILE.exists():
            with open(LAST_OPENED_DB_FILE, "r") as f:
                path_str = f.read().strip()
                if path_str:
                    path = Path(path_str)
                    if path.exists() and path.is_file():
                        return path
    except Exception as e:
        print(f"Error loading last opened database: {e}")
    return None

def fetch_videos_for_view(db_path_str: str, search_term: Optional[str] = None, 
                          page_number: int = 1, page_size: int = 10, 
                          sort_by: Optional[str] = 'last_updated_at', 
                          sort_direction: str = 'DESC') -> Dict:
    """ Fetches paginated and sorted video data from the database. """
    videos = []
    total_count = 0
    conn = None
    try:
        conn = sqlite3.connect(db_path_str)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        columns_to_select = [
            "id", "video_id", "title", "channel_id", "published_at", "status",
            "subtitle_status", "download_status", "transcription_status",
            "segmentation_10w_status", "analysis_status", "text_source",
            "ai_analysis_content", 
            "subtitle_file_path",
            "last_updated_at"
        ]
        select_cols_str = ', '.join(columns_to_select)

        base_query = "FROM videos"
        count_query_str = f"SELECT COUNT(*) as total_count {base_query}"
        
        params = []
        where_clauses = []

        if search_term:
            where_clauses.append("(title LIKE ? OR video_id LIKE ? OR channel_id LIKE ?)")
            params.extend([f"%{search_term}%", f"%{search_term}%", f"%{search_term}%"])
        
        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)
            count_query_str = f"SELECT COUNT(*) as total_count {base_query}"

        cursor.execute(count_query_str, params)
        count_row = cursor.fetchone()
        if count_row:
            total_count = count_row['total_count']

        offset = (page_number - 1) * page_size
        
        allowed_sort_columns = ["title", "published_at", "last_updated_at", "video_id", "channel_id"]
        if sort_by not in allowed_sort_columns:
            sort_by = 'last_updated_at'
        if sort_direction.upper() not in ['ASC', 'DESC']:
            sort_direction = 'DESC'

        paginated_query_str = f"SELECT {select_cols_str} {base_query} ORDER BY {sort_by} {sort_direction} LIMIT ? OFFSET ?"
        paginated_params = params + [page_size, offset]
        
        cursor.execute(paginated_query_str, paginated_params)
        rows = cursor.fetchall()

        for row in rows:
            videos.append(dict(row))
        
        print(f"Fetched {len(videos)} videos (page {page_number}/{ (total_count + page_size -1) // page_size if page_size > 0 else 1}) from {db_path_str} with search: '{search_term if search_term else 'N/A'}'")

    except sqlite3.Error as e:
        print(f"Database error in fetch_videos_for_view: {e}")
    except Exception as e:
        print(f"General error in fetch_videos_for_view: {e}")
    finally:
        if conn:
            conn.close()
    
    return {"videos": videos, "total_count": total_count} 