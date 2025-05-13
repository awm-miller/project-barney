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

### Using the HTML Command Generator (pipeline_interface.html)

To simplify the process of generating the command for `run_pipeline_single_table.py`, an HTML interface (`pipeline_interface.html`) is provided in the root of the project:

1.  **Open `pipeline_interface.html`** in your web browser.
2.  **Fill in the form fields** for Job Name, Channel IDs, Title Query, and other optional parameters for the pipeline run. The Download Directory defaults to `E:\video_downloads` but can be changed.
3.  Click the **"Generate Command"** button.
4.  The complete command-line instruction will be displayed, formatted for easy copying.
5.  **Copy this command** and paste it into your terminal (ensure you are in the root directory of the project) to execute the pipeline.

This interface helps avoid errors from manually typing out the command and its arguments, and provides default values for some options.

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


## Database Management

The `database_manager.py` script can be used for various database operations:

-   **Initialize Database (creates all tables and indexes):**
    ```bash
    python database_manager.py --initialize
    ```
-   **Reset Transcription Statuses (sets all to 'pending'):**
    ```bash
    python database_manager.py --reset-transcriptions
    ```
    (Other reset/maintenance functions are available within the script but may not have command-line triggers.)

## Key Dependencies

-   `google-api-python-client`: For Google APIs (YouTube, Drive).
-   `yt-dlp`: For downloading YouTube videos.
-   `python-dotenv`: For managing environment variables.
-   `google-cloud-speech` & `google-cloud-storage`: For Google Cloud services.
-   `google-generativeai`: For the Gemini API.

(See `requirements.txt` for the full list).

## Notes

-   The pipeline is designed to be somewhat resilient, with status tracking in the database allowing for retries or resumption of interrupted processes.
-   API quotas (YouTube, Google Cloud, Gemini) should be monitored, as extensive use can lead to temporary blocks. Some scripts include minor delays to help manage this, but careful planning for large datasets is advised.
-   The `VideoTableScope` in `run_pipeline_single_table.py` ensures that the main `videos` table is not directly modified by job runs, promoting data integrity and allowing for easier management of individual job results.
-   The `pipeline_interface.html` file provides a user-friendly way to generate the command for the main pipeline orchestrator (`run_pipeline_single_table.py`).
-   The `customtkinter` dependency in `requirements.txt` seems unused by the core CLI pipeline scripts. It might be for an auxiliary GUI tool not included in the provided file list.
-   The `.gitignore` file is quite broad for `*.txt` and `*.json`. Ensure necessary text/JSON data files (like `query.txt`, input CSVs, or specific non-credential JSONs) are not unintentionally ignored by adding specific exclusions (e.g., `!query.txt`) if needed.

