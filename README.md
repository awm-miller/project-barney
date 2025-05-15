# Video Processing and Analysis Pipeline

This project is a comprehensive pipeline for fetching, processing, analyzing, and exporting data for YouTube videos. It includes functionalities for subtitle retrieval and fixing, video downloading, transcription, AI-powered summarization, and structured data export.

## Features

*   **Subtitle Fetching:** Downloads subtitles for YouTube videos, with a preference for SRT format.
*   **Subtitle Conversion & Fixing:** Converts VTT to SRT and utilizes the `srt_fix` plugin with `yt-dlp` to correct overlapping subtitle timings.
*   **SRT to Timestamped Text:** Converts fixed SRT subtitles into plain text files while preserving timestamps (MM:SS Text format).
*   **Video Downloading:** Downloads videos directly as a fallback if subtitles are unavailable or processing fails.
*   **AI Summarization:** Uses Google's Gemini API to generate summaries of video content (from subtitles or ASR transcripts).
*   **Parallel Processing:** Employs multithreading for efficient batch processing in scripts like `fetch_subtitles.py`, `download_videos.py`, `ai_call.py`, and `export_to_csv.py`.
*   **Retry Logic:** Implements retry mechanisms for failed subtitle fetching attempts.
*   **Database Management:** Uses SQLite to track video metadata, processing status, and file paths.
*   **CSV Export:** Exports processed data into a CSV file, including titles, URLs, AI summaries, and links to relevant files (SRT, plain text subtitles, ASR transcripts) uploaded to Google Drive.
*   **Google Drive Integration:** Uploads subtitle files and ASR transcripts to a specified Google Drive folder during the CSV export process.
*   `run_pipeline.py`: Orchestrates the main processing pipeline, running various stages from subtitle fetching/conversion, video downloading/transcription/segmentation (as fallback), AI summarization, to CSV export.
    *   Provides a centralized way to run the common workflow.
    *   Accepts `--workers` argument for stages that support parallel processing.
    *   Allows skipping specific stages (e.g., `--skip_fetch_subtitles`, `--skip_ai_summarize`).
    *   Supports limit arguments for most stages (e.g. `--fetch_limit`, `--ai_max_videos`).
    *   Logs its own operations and the output of called scripts to `pipeline_run.log`.

## Setup

### Prerequisites

*   Python 3.x
*   `pip` (Python package installer)

### Installation

1.  **Clone the repository (if applicable):**
    ```bash
    git clone <your-repository-url>
    cd <your-repository-directory>
    ```

2.  **Create and activate a virtual environment (recommended):**
    ```bash
    python -m venv venv
    # On Windows
    venv\Scripts\activate
    # On macOS/Linux
    source venv/bin/activate
    ```

3.  **Install Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Install `yt-dlp` subtitle fixer plugin:**
    The `srt_fix` plugin is used by `fetch_subtitles.py` to correct overlapping subtitles (common on Youtube). Install it via pip:
    ```bash
    pip install https://github.com/bindestriche/srt_fix/archive/refs/heads/master.zip
    ```

5.  **Set up Environment Variables:**
    Create a `.env` file in the root of the project directory and add the following variables. Replace placeholder values with your actual credentials and IDs.

    ```env
    # For Google Cloud services (Speech-to-Text, Drive API)
    GOOGLE_APPLICATION_CREDENTIALS="path/to/your/serviceaccount_credentials.json"

    # For Gemini API (used in ai_call.py for summarization)
    GEMINI_API_KEY="YOUR_GEMINI_API_KEY"

    # For Google Drive export (export_to_csv.py)
    # Parent folder ID in Google Drive where export subfolders will be created
    GCLOUD_FOLDER="YOUR_GOOGLE_DRIVE_PARENT_FOLDER_ID"
    # Alternatively, you can use the more specific variable below if GCLOUD_FOLDER is used for other purposes
    # GOOGLE_DRIVE_PARENT_FOLDER_ID_FOR_CSV="YOUR_GOOGLE_DRIVE_PARENT_FOLDER_ID"

    # Optional: Directory for AI analysis output (used by ai_call.py, if it writes local files beyond DB updates)
    # ANALYSIS_DIR="./analysis_output" # Example path

    # Optional: Cookies file for yt-dlp if needed for certain videos
    # COOKIES_FILE_PATH="./cookies.txt"
    ```
    *   Ensure your `serviceaccount_credentials.json` has the necessary permissions for Google Drive (if uploading) and Google Cloud Speech-to-Text (if `transcribe_videos.py` is used).
    *   The `GEMINI_API_KEY` is required for the `ai_call.py` script.

6.  **Initialize the Database:**
    The pipeline uses an SQLite database (`pipeline_database.db`) to track video information and processing states.
    *   To create the database schema for the first time:
        ```bash
        python database_manager.py --initialize
        ```
    *   To re-initialize the database while preserving existing video and channel IDs but resetting processing statuses (useful for reprocessing):
        ```bash
        python database_manager.py --reinitialize-soft
        ```

## Pipeline Scripts Overview

The project consists of several Python scripts that form the processing pipeline:

*   `database_manager.py`: Manages the SQLite database schema and provides utility functions for database interaction.
*   `find_youtube_channels_by_keyword.py`: Tries to find Youtube channels based on keywords
*   `search_channel_videos_for_keyword.py`: Searches for videos within specific channels based on keywords.
*   `fetch_subtitles.py`: Fetches subtitles for videos from the database.
    *   Attempts to download SRT files directly.
    *   Uses `yt-dlp` with the `--convert-subs srt` flag.
    *   Employs the `srt_fix` postprocessor (`--use-postprocessor srt_fix:when=before_dl`) to fix timing issues, looking for `*-fixed.srt` files.
    *   Supports parallel fetching using the `--workers` argument.
    *   Includes retry logic for videos that initially fail or have 'unavailable' subtitle status.
    *   Use `--limit <number>` to process a specific number of videos.
*   `convert_subtitle_to_text.py`: Converts downloaded `.srt` subtitle files into plain text files with timestamps (`MM:SS Text`).
    *   Strips sequence numbers and manages blank lines.
    *   Use `--limit <number>` to process a specific number of videos.
*   `download_videos.py`: Downloads the actual video files. This is often used as a fallback if subtitles cannot be fetched.
    *   Supports parallel downloading using the `--workers` argument.
    *   Use `--limit <number>` to process a specific number of videos.
*   `transcribe_videos.py`: Transcribes audio from downloaded videos using a speech-to-text service (likely Google Cloud Speech).
*   `segment_transcripts_10w.py`: Segments ASR transcripts into smaller chunks (e.g., 10-word segments).
*   `ai_call.py`: Takes text input (either from plain text subtitles or segmented ASR transcripts) and generates an AI summary using the Gemini API.
    *   Supports parallel processing using the `--workers` argument.
*   `export_to_csv.py`: Exports processed video data to a CSV file.
    *   Uploads English fixed SRT files (`.en-fixed.srt`), Arabic plain text subtitle files, and conditionally ASR transcripts to a timestamped subfolder in Google Drive.
    *   The CSV includes video title, URL, AI summary, links to the uploaded files, and publication date.
    *   Supports parallel uploads using the `--workers` argument.
    *   Use `--no_upload` to disable Google Drive uploads.
*   `run_pipeline.py`: Orchestrates the main processing pipeline, running various stages from subtitle fetching/conversion, video downloading/transcription/segmentation (as fallback), AI summarization, to CSV export.
    *   Provides a centralized way to run the common workflow.
    *   Accepts `--workers` argument for stages that support parallel processing.
    *   Allows skipping specific stages (e.g., `--skip_fetch_subtitles`, `--skip_ai_summarize`).
    *   Supports limit arguments for most stages (e.g. `--fetch_limit`, `--ai_max_videos`).
    *   Logs its own operations and the output of called scripts to `pipeline_run.log`.

## Usage

The pipeline can be run by executing individual scripts or, more conveniently, by using the `run_pipeline.py` orchestrator script.

**Using the Orchestrator Script (`run_pipeline.py`):**

This is the recommended way to run the main processing flow.

1.  **Run the full pipeline (subtitle path, then transcription fallback if needed, summarization, and export):**
    ```bash
    python run_pipeline.py --workers 4
    ```

2.  **Run the pipeline, skipping subtitle fetching and conversion (forcing download/transcription path):**
    ```bash
    python run_pipeline.py --skip_fetch_subtitles --skip_convert_subtitles --workers 4
    ```

3.  **Run only the summarization and export steps:**
    ```bash
    python run_pipeline.py --skip_fetch_subtitles --skip_convert_subtitles --skip_download_videos --skip_transcribe_videos --skip_segment_transcripts --workers 4
    ```

4.  **Run with limits on specific stages:**
    ```bash
    python run_pipeline.py --workers 2 --fetch_limit 10 --ai_max_videos 5
    ```

Refer to the orchestrator's help for all available options:
```bash
python run_pipeline.py --help
```

**Example (running individual steps manually):**

While `run_pipeline.py` is preferred, individual scripts can still be run:

1.  **Fetch subtitles (with 4 workers, processing all eligible videos):**
    ```bash
    python fetch_subtitles.py --workers 4
    ```

2.  **Convert newly fetched SRTs to text (processing all eligible):**
    ```bash
    python convert_subtitle_to_text.py
    ```

3.  **Run AI summarization (with 4 workers, processing all eligible):**
    ```bash
    python ai_call.py --workers 4
    ```

4.  **Export data to CSV (with 4 workers for uploads):**
    ```bash
    python export_to_csv.py --workers 4
    ```

Refer to the arguments of each script (e.g., by running `python <script_name>.py --help`) for more specific options.

## Key Directories

*   `subtitles/`: Stores downloaded and fixed SRT subtitle files (e.g., `VIDEO_ID.lang-fixed.srt`).
*   `plain_text_subtitles/`: Stores plain text versions of subtitles (e.g., `VIDEO_ID.lang.txt`).
*   `videos/`: (Assumed) Default directory for downloaded video files.
*   `transcripts/`: (Assumed) Directory for ASR transcript files.

## Logging

Most scripts generate log files (e.g., `fetch_subtitles.log`, `summarize_transcripts.log`, `export_to_csv.log`) in the project's root directory. The `run_pipeline.py` script also creates `pipeline_run.log`, which captures the overall orchestration process and the output of the scripts it calls. These logs are crucial for monitoring and troubleshooting. 