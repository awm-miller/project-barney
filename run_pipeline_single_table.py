import argparse
import subprocess
import sys
import os
from datetime import datetime
import sqlite3
from typing import Optional

from database_manager import create_connection, DATABASE_NAME

# ---------------------------------------------
# Helper context-manager to temporarily swap the
# main `videos` table with a job-specific one.
# All existing pipeline scripts reference a table
# literally named `videos`.  To avoid touching
# each downstream script, we rename the archive
# table out of the way, create a fresh empty
# `videos` table for the job, then restore the
# original afterwards.
# ---------------------------------------------
class VideoTableScope:
    """Temporarily replace the `videos` table with a job-specific table."""

    def __init__(self, job_name: str):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.archive_table_name = f"videos_archive_{ts}"
        self.final_job_table_name = f"videos_{job_name}_{ts}"
        self.conn: Optional[sqlite3.Connection] = None

    def __enter__(self):
        self.conn = create_connection(DATABASE_NAME)
        if self.conn is None:
            raise RuntimeError("Could not open database connection.")

        cur = self.conn.cursor()
        # Disable FK checks – renaming tables that participate in FK
        # relationships breaks otherwise.
        cur.execute("PRAGMA foreign_keys = OFF;")
        self.conn.commit()

        # 1. Rename existing `videos` to the archive name
        cur.execute(f"ALTER TABLE videos RENAME TO {self.archive_table_name};")

        # 2. Create a brand-new empty `videos` table with the same schema
        schema_sql = cur.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?;",
            (self.archive_table_name,),
        ).fetchone()
        if schema_sql is None:
            raise RuntimeError("Could not fetch original `videos` table schema.")
        create_sql = schema_sql[0].replace(
            f"CREATE TABLE {self.archive_table_name}", "CREATE TABLE videos"
        )
        cur.execute(create_sql)
        self.conn.commit()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.conn is not None, "Connection missing in __exit__"
        cur = self.conn.cursor()
        # 3. Move job results away and restore archive table
        cur.execute(f"ALTER TABLE videos RENAME TO {self.final_job_table_name};")
        cur.execute(f"ALTER TABLE {self.archive_table_name} RENAME TO videos;")
        self.conn.commit()
        cur.execute("PRAGMA foreign_keys = ON;")
        self.conn.close()


def run_subprocess(script_name: str, args: list[str]):
    cmd = [sys.executable, script_name] + args
    print(f"\n[PIPELINE] Running: {' '.join(cmd)}\n", flush=True)
    subprocess.run(cmd, check=True)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Run the full video processing pipeline against an isolated "
            "job-specific videos table.  Existing data are preserved and "
            "re-attached when the run completes."
        )
    )
    parser.add_argument("--job-name", required=True, help="Short identifier for this job.")
    parser.add_argument(
        "--channels",
        required=True,
        help="Comma-separated list of YouTube channel IDs to search in",
    )
    parser.add_argument(
        "--title-query",
        required=True,
        help="String that must appear in the video title (passed to the search script)",
    )
    parser.add_argument(
        "--download-dir",
        default=os.getenv("DOWNLOAD_DIR", "downloads"),
        help="Directory where videos will be downloaded (default DOWNLOAD_DIR env or ./downloads)",
    )
    parser.add_argument("--workers", type=int, default=4, help="Parallel worker count for download stage.")
    parser.add_argument("--max-downloads", type=int, default=None, help="Optional cap on number of videos to download.")
    parser.add_argument(
        "--max-transcriptions", type=int, default=None, help="Optional cap on videos to transcribe in this run."
    )
    parser.add_argument(
        "--max-summaries", type=int, default=None, help="Optional cap on transcripts to summarise."
    )
    args = parser.parse_args()

    os.makedirs(args.download_dir, exist_ok=True)

    # ------------- EXECUTION PIPELINE --------------
    with VideoTableScope(args.job_name):
        # SEARCH
        run_subprocess(
            "search_channel_videos_for_keyword.py",
            ["--channels", args.channels, "--title-query", args.title_query],
        )
        # DOWNLOAD
        download_args = [
            "--download-dir",
            args.download_dir,
            "--workers",
            str(args.workers),
        ]
        if args.max_downloads:
            download_args += ["--limit", str(args.max_downloads)]
        run_subprocess("download_videos.py", download_args)

        # TRANSCRIBE
        transcribe_args = []
        if args.max_transcriptions:
            transcribe_args += ["--max-videos", str(args.max_transcriptions)]
        run_subprocess("transcribe_videos.py", transcribe_args)

        # AI CALL
        summarise_args = []
        if args.max_summaries:
            summarise_args += ["--max-videos", str(args.max_summaries)]
        run_subprocess("ai_call.py", summarise_args)

        # EXPORT CSV (optional)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_path = f"video_export_{args.job_name}_{timestamp}.csv"
        run_subprocess("export_to_csv.py", ["--output", export_path])

    print("\n[PIPELINE] Job completed successfully!\n")


if __name__ == "__main__":
    main() 