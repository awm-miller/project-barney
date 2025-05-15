#!/usr/bin/env python3

import subprocess
import argparse
import logging
import sys
from datetime import datetime

# --- Configuration ---
LOG_FILE = "pipeline_run.log"
DEFAULT_WORKERS = 4

# --- Logging Setup ---
# Ensure encoding is explicitly set for file handler
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, 'a', 'utf-8'),
        logging.StreamHandler(sys.stdout)  # To see output in console
    ]
)
logger = logging.getLogger(__name__)

def run_command(command_list: list[str], step_name: str) -> bool:
    """Runs a command, logs its output, and returns True on success."""
    command_str = " ".join(command_list)
    logger.info(f"Executing for {step_name}: {command_str}")
    try:
        # Using shell=False (default) is generally safer if command_list is well-defined.
        # Ensure python executable is correctly found, might need sys.executable
        python_executable = sys.executable # Use the same python interpreter that runs this script
        
        process = subprocess.run([python_executable] + command_list[1:], capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
        
        if process.stdout:
            logger.info(f"Stdout from {step_name} ({command_list[1]}):
{process.stdout.strip()}")
        if process.stderr:
            # Log stderr as warning, as some tools might output info to stderr
            logger.warning(f"Stderr from {step_name} ({command_list[1]}):
{process.stderr.strip()}")
        logger.info(f"{step_name} ({command_list[1]}) completed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {step_name} ({command_str}): Command exited with status {e.returncode}")
        if e.stdout:
            logger.error(f"Stdout from {step_name} ({command_list[1]}):
{e.stdout.strip()}")
        if e.stderr:
            logger.error(f"Stderr from {step_name} ({command_list[1]}):
{e.stderr.strip()}")
        return False
    except FileNotFoundError:
        logger.error(f"Error for {step_name}: The script {command_list[1]} was not found. Ensure it's in the correct path and executable.")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred with {step_name} ({command_str}): {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Run the main video processing pipeline for subtitles, transcription, summarization, and export.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Number of workers for parallel tasks (default: {DEFAULT_WORKERS}).")
    
    # Subtitle processing path
    parser.add_argument("--skip_fetch_subtitles", action="store_true", help="Skip fetching subtitles.")
    parser.add_argument("--skip_convert_subtitles", action="store_true", help="Skip converting subtitles to text.")
    
    # Transcription fallback path
    parser.add_argument("--skip_download_videos", action="store_true", help="Skip downloading videos (fallback if subtitles fail/skipped).")
    parser.add_argument("--skip_transcribe_videos", action="store_true", help="Skip transcribing downloaded videos.")
    parser.add_argument("--skip_segment_transcripts", action="store_true", help="Skip segmenting ASR transcripts.")
    
    # Common final steps
    parser.add_argument("--skip_ai_summarize", action="store_true", help="Skip AI summarization.")
    parser.add_argument("--skip_export_csv", action="store_true", help="Skip CSV export.")
    parser.add_argument("--export_no_upload", action="store_true", help="Disable Google Drive uploads during CSV export.")

    # Optional limits for individual stages (can be expanded)
    parser.add_argument("--fetch_limit", type=int, default=None, help="Limit number of videos for subtitle fetching.")
    parser.add_argument("--convert_limit", type=int, default=None, help="Limit number of videos for subtitle conversion.")
    parser.add_argument("--download_limit", type=int, default=None, help="Limit number of videos for downloading.")
    parser.add_argument("--transcribe_max_videos", type=int, default=None, help="Max videos for transcription step.")
    parser.add_argument("--segment_max_videos", type=int, default=None, help="Max videos for segmentation step.")
    parser.add_argument("--ai_max_videos", type=int, default=None, help="Max videos for AI summarization.")


    args = parser.parse_args()

    logger.info(f"--- Starting Main Pipeline Run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    logger.info(f"Workers for parallel tasks: {args.workers}")
    
    overall_success = True

    # --- Subtitle Path ---
    if not args.skip_fetch_subtitles:
        logger.info(">>> STAGE: Fetching Subtitles <<<")
        cmd = ["python", "fetch_subtitles.py", "--workers", str(args.workers)]
        if args.fetch_limit is not None:
            cmd.extend(["--limit", str(args.fetch_limit)])
        if not run_command(cmd, "Fetch Subtitles"):
            overall_success = False
            logger.warning("Fetching subtitles failed. Subsequent subtitle-dependent steps might be affected.")
    else:
        logger.info("Skipping: Fetching Subtitles")

    if not args.skip_convert_subtitles:
        if args.skip_fetch_subtitles:
            logger.warning("Skipping subtitle conversion as fetching was skipped.")
        else:
            logger.info(">>> STAGE: Converting Subtitles to Text <<<")
            cmd = ["python", "convert_subtitle_to_text.py"]
            if args.convert_limit is not None:
                cmd.extend(["--limit", str(args.convert_limit)])
            if not run_command(cmd, "Convert Subtitles"):
                overall_success = False
                logger.warning("Converting subtitles failed.")
    else:
        logger.info("Skipping: Converting Subtitles to Text")

    # --- Transcription Fallback Path ---
    # This path is typically for videos where subtitle processing wasn't successful or was skipped.
    # The individual scripts (download_videos, transcribe_videos) have their own logic 
    # to pick up videos based on database status.

    if not args.skip_download_videos:
        logger.info(">>> STAGE: Downloading Videos (Fallback) <<<")
        cmd = ["python", "download_videos.py", "--workers", str(args.workers)]
        if args.download_limit is not None:
            cmd.extend(["--limit", str(args.download_limit)])
        if not run_command(cmd, "Download Videos"):
            overall_success = False
            logger.warning("Downloading videos failed. Transcription and segmentation might be affected.")
    else:
        logger.info("Skipping: Downloading Videos")

    if not args.skip_transcribe_videos:
        if args.skip_download_videos:
            logger.warning("Skipping video transcription as video downloading was skipped.")
        else:
            logger.info(">>> STAGE: Transcribing Videos <<<")
            cmd = ["python", "transcribe_videos.py"] # transcribe_videos.py uses internal MAX_WORKERS
            if args.transcribe_max_videos is not None:
                 cmd.extend(["--max-videos", str(args.transcribe_max_videos)]) # Assuming it takes this
            if not run_command(cmd, "Transcribe Videos"):
                overall_success = False
                logger.warning("Transcribing videos failed. Segmentation might be affected.")
    else:
        logger.info("Skipping: Transcribing Videos")

    if not args.skip_segment_transcripts:
        if args.skip_download_videos or args.skip_transcribe_videos:
            logger.warning("Skipping transcript segmentation as prior steps (download/transcribe) were skipped or failed.")
        else:
            logger.info(">>> STAGE: Segmenting Transcripts <<<")
            cmd = ["python", "segment_transcripts_10w.py"] # segment_transcripts_10w.py is serial
            if args.segment_max_videos is not None:
                 cmd.extend(["--max-videos", str(args.segment_max_videos)])
            if not run_command(cmd, "Segment Transcripts"):
                overall_success = False
                logger.warning("Segmenting transcripts failed.")
    else:
        logger.info("Skipping: Segmenting Transcripts")

    # --- Final Common Stages ---
    if not args.skip_ai_summarize:
        logger.info(">>> STAGE: AI Summarization <<<")
        cmd = ["python", "ai_call.py", "--workers", str(args.workers)]
        if args.ai_max_videos is not None:
            cmd.extend(["--max_videos", str(args.ai_max_videos)]) # ai_call.py uses max_videos
        if not run_command(cmd, "AI Summarization"):
            overall_success = False
            logger.warning("AI summarization failed.")
    else:
        logger.info("Skipping: AI Summarization")

    if not args.skip_export_csv:
        logger.info(">>> STAGE: Exporting to CSV <<<")
        cmd = ["python", "export_to_csv.py", "--workers", str(args.workers)]
        if args.export_no_upload:
            cmd.append("--no_upload")
        if not run_command(cmd, "Export to CSV"):
            overall_success = False
            logger.warning("Exporting to CSV failed.")
    else:
        logger.info("Skipping: Exporting to CSV")

    logger.info(f"--- Main Pipeline Run Finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    if overall_success:
        logger.info("All executed pipeline stages completed successfully or were skipped.")
    else:
        logger.error("One or more pipeline stages failed. Please check the logs.")
        sys.exit(1) # Exit with error code if any step failed

if __name__ == "__main__":
    main() 