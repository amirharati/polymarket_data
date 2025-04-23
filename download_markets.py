"""
Script to download market data from the Polymarket Gamma API in batches.

Fetches markets based on status and optional date filters, saving each batch
to a JSON Lines (.jsonl) file named with the offset. Resumes automatically
by scanning existing filenames in the output directory.
"""

import requests
import json
import time
import argparse
import logging
import os
import re
from pathlib import Path

# --- Constants ---
GAMMA_API_BASE_URL = "https://gamma-api.polymarket.com"
DEFAULT_LIMIT = 20
DEFAULT_STATUS = 'closed'
DEFAULT_SLEEP_TIME = 1.0 # Seconds between requests

# --- Helper Functions ---
def setup_logging(log_file_path):
    """Configures logging to both console and file."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Prevent adding handlers multiple times if script/function is called again
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # File Handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

def get_starting_offset(output_dir, limit):
    """Scans output directory for saved batch files to determine starting offset."""
    max_saved_offset = -limit # Start from offset 0 if no files found
    # Revert back to raw string for regex
    pattern = re.compile(r"markets_offset_(\d+)_limit_\d+\.jsonl")
    try:
        if output_dir.exists():
            for filename in output_dir.iterdir():
                match = pattern.match(filename.name)
                if match:
                    offset = int(match.group(1))
                    # Check if file has content (simple check)
                    if filename.stat().st_size > 0:
                         max_saved_offset = max(max_saved_offset, offset)
                    else:
                        logging.warning(f"Found empty or potentially incomplete file: {filename.name}. Ignoring for offset calculation.")


        starting_offset = max_saved_offset + limit
        logging.info(f"Determined starting offset: {starting_offset} (based on max saved offset: {max_saved_offset})")
        return starting_offset
    except Exception as e:
        logging.error(f"Error scanning output directory {output_dir} for offset: {e}")
        logging.warning("Defaulting to starting offset 0.")
        return 0

def fetch_markets_batch(offset, limit, status_filter, date_filters):
    """Fetches a single batch of markets from the API."""
    url = f"{GAMMA_API_BASE_URL}/markets"
    params = {
        'limit': limit,
        'offset': offset
    }

    # Add status filter
    if status_filter == 'closed':
        params['closed'] = 'true'
    elif status_filter == 'open':
        params['closed'] = 'false' # Assuming closed=false means open
        # Alternatively, could use active=true, but closed seems more definitive for our goal later
    elif status_filter != 'all':
        logging.warning(f"Invalid status filter '{status_filter}'. Fetching all statuses.")

    # Add optional date filters
    if date_filters.get('start_date_min'): params['start_date_min'] = date_filters['start_date_min']
    if date_filters.get('start_date_max'): params['start_date_max'] = date_filters['start_date_max']
    if date_filters.get('end_date_min'): params['end_date_min'] = date_filters['end_date_min']
    if date_filters.get('end_date_max'): params['end_date_max'] = date_filters['end_date_max']

    logging.info(f"Fetching batch: offset={offset}, limit={limit}, params={params}")
    try:
        response = requests.get(url, params=params, timeout=30) # Added timeout
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        logging.info(f"Received {len(data)} markets for offset {offset}.")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Network or HTTP error fetching offset {offset}: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON for offset {offset}: {e}")
        logging.error(f"Response text (first 500 chars): {response.text[:500]}")
        return None

def save_batch_jsonl(batch_data, output_dir, offset, limit):
    """Saves a batch of market data to a JSON Lines file using a temp file."""
    if not batch_data:
        logging.warning(f"No data to save for offset {offset}.")
        return False

    final_filename = output_dir / f"markets_offset_{offset}_limit_{limit}.jsonl"
    temp_filename = final_filename.with_suffix('.jsonl.tmp')

    try:
        with open(temp_filename, 'w', encoding='utf-8') as f:
            for market in batch_data:
                # Ensure each market is written as a single line
                json.dump(market, f, ensure_ascii=False)
                f.write('\n')

        # Rename temp file to final name after successful write
        os.rename(temp_filename, final_filename)
        logging.info(f"Successfully saved batch to {final_filename}")
        return True
    except IOError as e:
        logging.error(f"Error writing batch file {temp_filename} or renaming to {final_filename}: {e}")
        # Attempt to clean up temp file if it exists
        if temp_filename.exists():
            try:
                os.remove(temp_filename)
            except OSError as remove_e:
                logging.error(f"Could not remove temporary file {temp_filename}: {remove_e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during saving batch offset {offset}: {e}")
        return False


# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download market data from Polymarket Gamma API.")
    parser.add_argument("--output-dir", type=str, required=True,
                        help="Directory to store the output JSON Lines files.")
    parser.add_argument("--status", type=str, default=DEFAULT_STATUS,
                        choices=['closed', 'open', 'all'],
                        help=f"Market status to fetch (default: {DEFAULT_STATUS}).")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Number of markets per batch/file (default: {DEFAULT_LIMIT}).")
    parser.add_argument("--log-file", type=str, default="market_downloader.log",
                        help="Path to the log file.")
    parser.add_argument("--sleep-time", type=float, default=DEFAULT_SLEEP_TIME,
                        help=f"Seconds to sleep between API requests (default: {DEFAULT_SLEEP_TIME}).")
    # Optional date filters (based on scheduled start/end dates)
    parser.add_argument("--start-date-min", type=str, help="Filter by minimum start date (YYYY-MM-DD).")
    parser.add_argument("--start-date-max", type=str, help="Filter by maximum start date (YYYY-MM-DD).")
    parser.add_argument("--end-date-min", type=str, help="Filter by minimum end date (YYYY-MM-DD).")
    parser.add_argument("--end-date-max", type=str, help="Filter by maximum end date (YYYY-MM-DD).")

    args = parser.parse_args()

    setup_logging(args.log_file)
    logging.info("--- Starting Market Downloader Script ---")
    logging.info(f"Arguments: {vars(args)}")

    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    date_filters = {
        'start_date_min': args.start_date_min,
        'start_date_max': args.start_date_max,
        'end_date_min': args.end_date_min,
        'end_date_max': args.end_date_max,
    }

    current_offset = get_starting_offset(output_path, args.limit)

    while True:
        batch = fetch_markets_batch(current_offset, args.limit, args.status, date_filters)

        if batch is None:
            logging.error(f"Failed to fetch batch at offset {current_offset}. Stopping.")
            break # Stop on fetch error

        if not batch:
            logging.info("Received empty batch. Assuming pagination complete.")
            break # Stop when API returns empty list

        # Attempt to save the batch
        save_successful = save_batch_jsonl(batch, output_path, current_offset, args.limit)

        if not save_successful:
            logging.error(f"Failed to save batch for offset {current_offset}. Stopping to prevent inconsistent state.")
            break # Stop if saving fails

        # Prepare for next iteration
        current_offset += args.limit
        logging.info(f"Sleeping for {args.sleep_time} seconds...")
        time.sleep(args.sleep_time)

    logging.info("--- Market Downloader Script Finished ---") 