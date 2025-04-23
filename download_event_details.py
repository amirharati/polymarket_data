"""
Script to download event details from the Polymarket Gamma API.

Scans market data files (.jsonl) to extract unique event IDs, then fetches
details for each event using the /events/{id} endpoint. Saves each event's
details to a separate JSON file (event_{id}.json). Resumes automatically
by checking for existing event files in the output directory.
"""

import requests
import json
import time
import argparse
import logging
import os
from pathlib import Path
import concurrent.futures # Added for parallel execution

# --- Constants ---
GAMMA_API_BASE_URL = "https://gamma-api.polymarket.com"
DEFAULT_SLEEP_TIME = 1.0  # Kept for potential future use, but not primary throttling
NUM_WORKERS = 8  # Number of parallel download threads

# --- Helper Functions ---
def setup_logging(log_file_path):
    """Configures logging to both console and file."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Prevent adding handlers multiple times
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

def extract_unique_event_ids(market_data_dir):
    """
    Scans market data directory for .jsonl files and extracts unique event IDs
    from the 'events' array within each market JSON object.
    """
    event_ids = set()
    market_data_path = Path(market_data_dir)

    if not market_data_path.is_dir():
        logging.error(f"Market data directory not found: {market_data_dir}")
        return event_ids

    logging.info(f"Scanning directory for market files: {market_data_dir}")
    jsonl_files = list(market_data_path.glob('markets_offset_*.jsonl'))
    logging.info(f"Found {len(jsonl_files)} potential market data files.")

    for file_path in jsonl_files:
        logging.debug(f"Processing file: {file_path.name}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        market_data = json.loads(line.strip())
                        # Check if 'events' key exists and is a list
                        if isinstance(market_data.get('events'), list):
                            for event in market_data['events']:
                                # Check if event is a dict and has an 'id'
                                if isinstance(event, dict) and 'id' in event:
                                    event_ids.add(str(event['id'])) # Ensure ID is string
                                else:
                                     logging.warning(f"Skipping invalid event structure in {file_path.name}, line {line_num}: {event}")
                        # Allow markets without an 'events' array or where it's not a list
                        elif 'events' in market_data:
                             logging.debug(f"Market in {file_path.name}, line {line_num} has non-list 'events' field: {type(market_data.get('events'))}")

                    except json.JSONDecodeError as e:
                        logging.error(f"JSON decode error in {file_path.name}, line {line_num}: {e}")
                    except Exception as e:
                         logging.error(f"Unexpected error processing line {line_num} in {file_path.name}: {e}")
        except IOError as e:
            logging.error(f"Could not read file {file_path.name}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error processing file {file_path.name}: {e}")


    logging.info(f"Extracted {len(event_ids)} unique event IDs.")
    return event_ids

def fetch_event_details(event_id):
    """Fetches details for a single event from the API."""
    url = f"{GAMMA_API_BASE_URL}/events/{event_id}"
    logging.debug(f"Fetching details for event ID: {event_id} from {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        logging.debug(f"Successfully fetched details for event ID: {event_id}")
        return data
    except requests.exceptions.Timeout:
        logging.error(f"Timeout error fetching event ID {event_id}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Network or HTTP error fetching event ID {event_id}: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON for event ID {event_id}: {e}")
        logging.error(f"Response text (first 500 chars): {response.text[:500]}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error fetching event {event_id}: {e}")
        return None

def save_event_details(event_data, output_dir):
    """Saves event detail data to a JSON file using a temp file."""
    if not event_data or not isinstance(event_data, dict) or 'id' not in event_data:
        logging.error(f"Invalid event data received, cannot save: {event_data}")
        return False

    event_id = str(event_data['id']) # Ensure ID is string for filename consistency
    output_path = Path(output_dir)
    final_filename = output_path / f"event_{event_id}.json"
    temp_filename = final_filename.with_suffix('.json.tmp')

    try:
        # Ensure output directory exists
        output_path.mkdir(parents=True, exist_ok=True)

        with open(temp_filename, 'w', encoding='utf-8') as f:
            json.dump(event_data, f, ensure_ascii=False, indent=4) # Indent for readability

        # Rename temp file to final name after successful write
        os.rename(temp_filename, final_filename)
        logging.debug(f"Successfully saved event details to {final_filename}")
        return True
    except IOError as e:
        logging.error(f"Error writing event file {temp_filename} or renaming to {final_filename}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during saving event ID {event_id}: {e}")

    # Attempt to clean up temp file if it exists and saving failed
    if temp_filename.exists():
        try:
            os.remove(temp_filename)
            logging.warning(f"Removed temporary file {temp_filename} after save error.")
        except OSError as remove_e:
            logging.error(f"Could not remove temporary file {temp_filename}: {remove_e}")
    return False

# --- Worker Function ---
def fetch_and_save_event(event_id, output_dir):
    """Worker function to fetch and save details for a single event."""
    logging.debug(f"Worker started for event ID: {event_id}")
    event_details = fetch_event_details(event_id)
    if event_details is None:
        logging.error(f"Worker failed to fetch event ID {event_id}")
        return event_id, "fetch_error"

    save_successful = save_event_details(event_details, output_dir)
    if save_successful:
        logging.debug(f"Worker successfully saved event ID {event_id}")
        return event_id, "success"
    else:
        logging.error(f"Worker failed to save event ID {event_id}")
        return event_id, "save_error"

# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download event details from Polymarket Gamma API in parallel.")
    parser.add_argument("--market-data-dir", type=str, required=True,
                        help="Directory containing the market data .jsonl files.")
    parser.add_argument("--output-dir", type=str, required=True,
                        help="Directory to store the output event detail JSON files.")
    parser.add_argument("--log-file", type=str, default="event_downloader.log",
                        help="Path to the log file.")
    parser.add_argument("--sleep-time", type=float, default=DEFAULT_SLEEP_TIME,
                        help=f"Seconds to sleep between API requests (less relevant with parallel execution).")
    parser.add_argument("--workers", type=int, default=NUM_WORKERS,
                        help=f"Number of parallel download workers (default: {NUM_WORKERS}).")

    args = parser.parse_args()

    setup_logging(args.log_file)
    logging.info("--- Starting Event Details Downloader Script ---")
    logging.info(f"Arguments: {vars(args)}")

    output_path = Path(args.output_dir)
    # Ensure output directory exists *before* extraction, though save also checks
    output_path.mkdir(parents=True, exist_ok=True)

    # --- Phase 1: Extract all unique event IDs ---
    unique_event_ids = extract_unique_event_ids(args.market_data_dir)

    if not unique_event_ids:
        logging.warning("No event IDs extracted. Exiting.")
        exit()

    logging.info(f"Found {len(unique_event_ids)} unique event IDs in source files.")

    # --- Phase 2: Filter IDs that need fetching ---
    ids_to_fetch = []
    skipped_count = 0
    for event_id in unique_event_ids:
        event_file_path = output_path / f"event_{event_id}.json"
        if event_file_path.exists() and event_file_path.stat().st_size > 0:
            skipped_count += 1
        else:
            ids_to_fetch.append(event_id)

    total_ids = len(unique_event_ids)
    needed_count = len(ids_to_fetch)
    logging.info(f"Already downloaded: {skipped_count}. Need to fetch: {needed_count}.")

    if not ids_to_fetch:
        logging.info("All event details already downloaded. Exiting.")
        exit()

    # --- Phase 3: Fetch and Save details in parallel ---
    processed_count = 0
    error_count = 0
    fetch_errors = []
    save_errors = []

    logging.info(f"Starting parallel fetching with {args.workers} workers...")

    # Use ThreadPoolExecutor for parallel execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Create a future for each ID to fetch
        future_to_id = {executor.submit(fetch_and_save_event, event_id, args.output_dir): event_id for event_id in ids_to_fetch}

        for future in concurrent.futures.as_completed(future_to_id):
            event_id = future_to_id[future]
            try:
                _id, status = future.result()
                if status == "success":
                    processed_count += 1
                    logging.info(f"Completed {processed_count}/{needed_count} ({status}) ID: {_id}")
                elif status == "fetch_error":
                    error_count += 1
                    fetch_errors.append(_id)
                    logging.warning(f"Completed {processed_count}/{needed_count} ({status}) ID: {_id}")
                elif status == "save_error":
                    error_count += 1
                    save_errors.append(_id)
                    logging.warning(f"Completed {processed_count}/{needed_count} ({status}) ID: {_id}")
            except Exception as exc:
                error_count += 1
                fetch_errors.append(event_id) # Assume fetch error if exception in worker
                logging.error(f"Event ID {event_id} generated an exception: {exc}")

    logging.info("--- Event Details Downloader Script Finished ---")
    logging.info(f"Total unique IDs found: {total_ids}")
    logging.info(f"Already existed / Skipped: {skipped_count}")
    logging.info(f"Attempted to fetch: {needed_count}")
    logging.info(f"Successfully fetched & saved: {processed_count}")
    logging.info(f"Fetch errors: {len(fetch_errors)}")
    logging.info(f"Save errors: {len(save_errors)}")
    if fetch_errors:
        logging.warning(f"Event IDs with fetch errors (will retry on next run): {fetch_errors}")
    if save_errors:
        logging.warning(f"Event IDs with save errors (will retry on next run): {save_errors}") 