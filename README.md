# YouTube Video Processing Pipeline

This project is a Python-based pipeline for searching, downloading, transcribing, summarizing, and exporting data about YouTube videos. It is designed to process videos in stages, using a central SQLite database to manage the state of each video throughout the pipeline.

## Features

- **Channel Discovery**: Find YouTube channel IDs based on a list of institution names.
- **Targeted Video Search**: Search for videos within specific channels based on keywords in video titles.
- **Video Downloading**: Download videos using `yt-dlp`.
- **Arabic Speech-to-Text**: Transcribe audio from videos into Arabic with word-level timestamps using Google Cloud Speech-to-Text.
- **Transcript Segmentation**: Process word-level transcripts into 10-word segments.
- **AI Summarization**: Generate concise English summaries from Arabic transcripts using Google's Gemini API.
- **Data Export**: Export processed data (summaries, video details) to CSV, and upload transcripts to Google Drive.
- **Database-Driven Workflow**: A SQLite database tracks video status and metadata across all processing stages.
- **Isolated Job Execution**: Run the entire pipeline for specific "jobs," with results stored in separate, versioned tables.
- **Modular Scripts**: Each stage of the pipeline is handled by a dedicated Python script.

## Project Structure

The pipeline consists of several core Python scripts:

-   **`database_manager.py`**: Defines and manages the SQLite database schema (`pipeline_database.db`).
-   **`find_youtube_channels_by_keyword.py`**: Discovers YouTube channels based on institution names.
-   **`search_channel_videos_for_keyword.py`**: Searches for videos within channels by title keywords.
-   **`download_videos.py`**: Downloads videos listed in the database.
-   **`transcribe_videos.py`**: Extracts audio, transcribes it using Google Cloud Speech-to-Text.
-   **`segment_transcripts_10w.py`**: Segments word-level transcripts into 10-word chunks.
-   **`ai_call.py` (summarize_transcripts.py)**: Generates summaries using the Gemini API.
-   **`export_to_csv.py`**: Exports data to CSV and uploads transcripts to Google Drive.
-   **`run_pipeline_single_table.py`**: Orchestrates the execution of the entire pipeline for a specific job, ensuring data isolation.

Utility scripts:
-   **`retry_pending_downloads.py`**: Retries downloading videos marked as 'pending'.
-   **`list_videos_by_date.py`**: Lists videos from the database, sorted by publication date.
-   **`channel_search.py`**: A wrapper to quickly run `search_channel_videos_for_keyword.py` for a hardcoded channel and a query from `query.txt`.

## Prerequisites

-   Python 3.x
-   `ffmpeg` installed and accessible in the system PATH (for audio extraction).
-   Google Cloud Platform account with:
    -   Speech-to-Text API enabled.
    -   Cloud Storage API enabled (for temporary audio storage).
    -   Service account credentials (`serviceaccount_credentials.json`).
-   Google Gemini API key.
-   YouTube Data API key.
-   Google Drive API enabled (for transcript export).

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\\Scripts\\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**
    Create a `.env` file in the root directory of the project and populate it with the necessary API keys, GCS bucket details, and directory paths. Example:
    ```env
    YOUTUBE_API_KEY="YOUR_YOUTUBE_API_KEY"
    GEMINI_API_KEY="YOUR_GEMINI_API_KEY"
    GOOGLE_APPLICATION_CREDENTIALS="path/to/your/serviceaccount_credentials.json"

    DOWNLOAD_DIR="path/to/your/download_directory"
    TRANSCRIPTS_DIR="path/to/your/transcripts_directory" # For word-level transcripts
    DEFAULT_SEGMENTED_TRANSCRIPTS_DIR="path/to/your/segmented_transcripts_directory" # Used by segment_transcripts_10w.py
    ANALYSIS_DIR="path/to/your/analysis_output_directory" # For AI summaries (if saved as files, though current script saves to DB)

    GCS_BUCKET_NAME="your-gcs-bucket-name"
    # Parent Google Drive folder ID for CSV export transcript uploads
    GCLOUD_FOLDER="YOUR_GOOGLE_DRIVE_PARENT_FOLDER_ID"
    # OR (more specific, overrides GCLOUD_FOLDER for CSV export if present)
    # GOOGLE_DRIVE_PARENT_FOLDER_ID_FOR_CSV="YOUR_DRIVE_PARENT_FOLDER_ID_FOR_CSV"
    ```
    Ensure the specified directories exist or the scripts have permissions to create them.

5.  **Initialize the Database:**
    Run the database manager to create the necessary tables:
    ```bash
    python database_manager.py --initialize
    ```

## Running the Pipeline

The main way to run the full pipeline for a specific set of inputs is using `run_pipeline_single_table.py`.

### Using the HTML Command Generator (Recommended for `run_pipeline_single_table.py`)

A user-friendly way to generate the command for `run_pipeline_single_table.py` is by using the included `pipeline_interface.html` file:

1.  **Open `pipeline_interface.html` in your web browser.**
2.  Fill in the required fields (Job Name, Channel IDs, Title Query) and any optional parameters for the pipeline run.
3.  Click the "Generate Command" button.
4.  The fully constructed command will appear in the text area below the button.
5.  Copy this command.
6.  Paste and run it in your terminal from the root directory of the project.

This interface helps prevent errors in typing out the command and its various arguments.

### Manual Command Line Execution for `run_pipeline_single_table.py`

If you prefer, you can construct the command manually:

```bash
python run_pipeline_single_table.py \\
    --job-name "my_specific_job" \\
    --channels "CHANNEL_ID_1,CHANNEL_ID_2" \\
    --title-query "keyword_in_title" \\
    --download-dir "./downloads" \\
    --workers 4 \\
    --max-downloads 10 \\
    --max-transcriptions 5 \\
    --max-summaries 5
```

**Explanation of Arguments for `run_pipeline_single_table.py`:**
-   `--job-name`: A unique name for this pipeline run. Results will be stored in a table like `videos_my_specific_job_TIMESTAMP`.
-   `--channels`: Comma-separated YouTube channel IDs to search.
-   `--title-query`: Keyword(s) to search for in video titles.
-   `--download-dir`: Directory to save downloaded videos.
-   `--workers`: Number of parallel workers for the download stage.
-   `--max-downloads` (optional): Limit the number of videos to download.
-   `--max-transcriptions` (optional): Limit the number of videos to transcribe.
-   `--max-summaries` (optional): Limit the number of transcripts to summarize.

### Individual Script Execution

You can also run individual scripts for specific tasks if needed. Refer to each script's `--help` option for its specific arguments. For example:

-   **Find Channels:**
    ```bash
    python find_youtube_channels_by_keyword.py --institutions unique_institutions.csv
    ```
    (Ensure `unique_institutions.csv` contains one institution name per line.)

-   **Download Videos (processes videos not yet 'completed'):**
    ```bash
    python download_videos.py --download-dir ./downloads --workers 4 --limit 20
    ```

-   **Transcribe Videos:**
    ```bash
    python transcribe_videos.py --max-videos 10
    ```

-   **Segment Transcripts:**
    ```bash
    python segment_transcripts_10w.py --output-dir ./transcripts_segmented --max-videos 10
    ```

-   **Summarize Transcripts (AI Call):**
    ```bash
    python ai_call.py --max-videos 10 --max-workers 2
    ```

-   **Export to CSV:**
    ```bash
    python export_to_csv.py --output_csv "export_run_$(date +%Y%m%d).csv"
    ```
    *(Note: `export_to_csv.py` uses `--output_csv`, while `run_pipeline_single_table.py` calls it with `--output` which might need adjustment in the orchestrator or the script for consistency.)*

## Web Interface (Command Generator)

This project includes `pipeline_interface.html`, a simple web page that acts as a command generator for the main pipeline script (`run_pipeline_single_table.py`).

**How to Use:**

1.  Open `pipeline_interface.html` directly in your web browser (no web server needed).
2.  Fill in the form fields corresponding to the arguments for `run_pipeline_single_table.py`:
    *   Job Name (required)
    *   Channel IDs (comma-separated, required)
    *   Title Query (required)
    *   Download Directory (defaults to `E:\video_downloads`)
    *   Number of Workers (defaults to 4)
    *   Max Downloads, Max Transcriptions, Max Summaries (optional)
3.  Click the "Generate Command" button.
4.  The complete command-line instruction will be displayed in the text area.
5.  Copy this command and run it in your terminal from the root directory of the project.

This interface simplifies the process of configuring and starting a pipeline run, reducing the chance of typos in the command-line arguments.

## Database Management

The `database_manager.py`