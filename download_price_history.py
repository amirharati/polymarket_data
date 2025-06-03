"""
Script to download price history time series data from the Polymarket CLOB API.

Scans market detail JSON files (market_{id}.json) to extract the market ID
and the first CLOB token ID (assumed to be the 'Yes' outcome). Fetches the
price history for that token ID using the /prices-history endpoint and saves
the raw JSON response to a file (price_history_yes_{market_id}.json).
Resumes automatically by checking for existing price history files.
"""

import requests
import json
import time
import argparse
import logging
import os
from pathlib import Path
import concurrent.futures # For parallel execution

# --- Constants ---
# Note: Using the clob subdomain as specified in the example URL
CLOB_API_BASE_URL = "https://clob.polymarket.com"
DEFAULT_SLEEP_TIME = 0.1 # Reduced sleep as parallelism handles rate limiting better
NUM_WORKERS = 8  # Number of parallel download threads
START_TS = 0 # Start timestamp for fetching history (beginning of time)
END_TS = 2000000000 # End timestamp (far future, e.g., year 2033) to get all history

# --- Helper Functions ---
def setup_logging(log_file_path):
    """Configures logging to both console and file."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s') # Added thread name
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Prevent adding handlers multiple times
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # File Handler
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8') # Specify encoding
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

def extract_market_and_token_ids(market_details_dir):
    """
    Scans market details directory for market_*.json files and extracts
    (market_id, first_clob_token_id) pairs.
    """
    market_token_pairs = []
    market_details_path = Path(market_details_dir)

    if not market_details_path.is_dir():
        logging.error(f"Market details directory not found: {market_details_dir}")
        return market_token_pairs

    logging.info(f"Scanning directory for market detail files: {market_details_dir}")
    json_files = list(market_details_path.glob('market_*.json'))
    logging.info(f"Found {len(json_files)} potential market detail files.")

    processed_files = 0
    skipped_files_no_tokens = 0
    skipped_files_parse_error = 0

    for file_path in json_files:
        logging.debug(f"Processing file: {file_path.name}")
        market_id = file_path.stem.split('_')[-1] # Extract ID from filename market_{id}
        first_token_id = None

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                market_data = json.load(f)
                clob_token_ids_str = market_data.get('clobTokenIds')

                if not clob_token_ids_str:
                    logging.debug(f"Skipping {file_path.name}: 'clobTokenIds' field is missing or empty.")
                    skipped_files_no_tokens += 1
                    continue

                try:
                    # Parse the string representation of the list
                    token_list = json.loads(clob_token_ids_str)
                    if isinstance(token_list, list) and len(token_list) > 0:
                        first_token_id = str(token_list[0]) # Take the first one, ensure string
                    else:
                        logging.warning(f"Skipping {file_path.name}: 'clobTokenIds' did not contain a valid list or was empty after parsing. Content: {clob_token_ids_str}")
                        skipped_files_no_tokens += 1
                        continue
                except json.JSONDecodeError:
                    logging.error(f"Skipping {file_path.name}: Could not parse 'clobTokenIds' JSON string: {clob_token_ids_str}")
                    skipped_files_no_tokens += 1
                    continue
                except Exception as parse_e:
                     logging.error(f"Skipping {file_path.name}: Unexpected error parsing 'clobTokenIds': {parse_e}. Content: {clob_token_ids_str}")
                     skipped_files_no_tokens += 1
                     continue

            # If we got a token ID, add the pair
            if first_token_id:
                market_token_pairs.append((market_id, first_token_id))
                processed_files += 1

        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error reading file {file_path.name}: {e}")
            skipped_files_parse_error += 1
        except IOError as e:
            logging.error(f"Could not read file {file_path.name}: {e}")
            skipped_files_parse_error += 1
        except Exception as e:
            logging.error(f"Unexpected error processing file {file_path.name}: {e}")
            skipped_files_parse_error += 1

    logging.info(f"Successfully extracted token IDs for {processed_files} markets.")
    if skipped_files_no_tokens > 0:
        logging.warning(f"Skipped {skipped_files_no_tokens} files due to missing/invalid 'clobTokenIds'.")
    if skipped_files_parse_error > 0:
        logging.warning(f"Skipped {skipped_files_parse_error} files due to JSON read/parse errors.")

    return market_token_pairs

def fetch_price_history(clob_token_id):
    """Fetches price history for a single CLOB token ID."""
    url = f"{CLOB_API_BASE_URL}/prices-history?market={clob_token_id}&startTs={START_TS}&endTs={END_TS}"
    logging.debug(f"Fetching price history for token ID: {clob_token_id}") # Don't log full URL to avoid large token IDs in logs
    try:
        response = requests.get(url, timeout=60) # Increased timeout for potentially large history
        response.raise_for_status()
        data = response.json()
        # Basic validation of response structure
        if isinstance(data, dict) and 'history' in data and isinstance(data['history'], list):
            logging.debug(f"Successfully fetched price history for token ID: {clob_token_id}. Records: {len(data['history'])}")
            return data
        else:
             logging.error(f"Invalid JSON structure received for token ID {clob_token_id}. Missing 'history' list. Response: {str(data)[:500]}")
             return None
    except requests.exceptions.Timeout:
        logging.error(f"Timeout error fetching price history for token ID {clob_token_id}")
        return None
    except requests.exceptions.RequestException as e:
        # Handle specific errors like 404 Not Found if needed
        if e.response is not None and e.response.status_code == 404:
             logging.warning(f"404 Not Found fetching price history for token ID {clob_token_id}. May not have history.")
        else:
             logging.error(f"Network or HTTP error fetching price history for token ID {clob_token_id}: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON for price history, token ID {clob_token_id}: {e}")
        logging.error(f"Response text (first 500 chars): {response.text[:500]}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error fetching price history for token {clob_token_id}: {e}")
        return None

def save_price_history(price_data, market_id, output_dir):
    """Saves price history data to a JSON file using a temp file."""
    if not price_data or not isinstance(price_data, dict) or 'history' not in price_data:
        logging.error(f"Invalid price data received for market {market_id}, cannot save: {price_data}")
        return False

    output_path = Path(output_dir)
    final_filename = output_path / f"price_history_yes_{market_id}.json"
    temp_filename = final_filename.with_suffix('.json.tmp')

    try:
        output_path.mkdir(parents=True, exist_ok=True)
        with open(temp_filename, 'w', encoding='utf-8') as f:
            # Save raw JSON, no indentation needed for potentially large data
            json.dump(price_data, f, ensure_ascii=False)

        os.rename(temp_filename, final_filename)
        logging.debug(f"Successfully saved price history to {final_filename}")
        return True
    except IOError as e:
        logging.error(f"Error writing price history file {temp_filename} or renaming to {final_filename}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during saving price history for market ID {market_id}: {e}")

    # Attempt to clean up temp file if it exists and saving failed
    if temp_filename.exists():
        try:
            os.remove(temp_filename)
            logging.warning(f"Removed temporary file {temp_filename} after save error.")
        except OSError as remove_e:
            logging.error(f"Could not remove temporary file {temp_filename}: {remove_e}")
    return False

# --- Worker Function ---
def fetch_and_save_price_history(market_id, clob_token_id, output_dir):
    """Worker function to fetch and save price history for a single market."""
    logging.debug(f"Worker started for market ID: {market_id} (token: {clob_token_id})")
    price_history_data = fetch_price_history(clob_token_id)
    if price_history_data is None:
        # Fetch error includes cases like 404 where data might legitimately not exist
        logging.warning(f"Worker did not fetch or received invalid price history for market {market_id} (token: {clob_token_id})")
        return market_id, "fetch_error_or_no_data" # Treat 404 or bad data as a fetch issue for retry logic

    save_successful = save_price_history(price_history_data, market_id, output_dir)
    if save_successful:
        logging.debug(f"Worker successfully saved price history for market ID {market_id}")
        return market_id, "success"
    else:
        logging.error(f"Worker failed to save price history for market ID {market_id}")
        return market_id, "save_error"

# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download price history from Polymarket CLOB API in parallel.")
    parser.add_argument("--market-details-dir", type=str, required=True,
                        help="Directory containing the market detail (market_{id}.json) files.")
    parser.add_argument("--output-dir", type=str, required=True,
                        help="Directory to store the output price history JSON files (price_history_yes_{market_id}.json).")
    parser.add_argument("--log-file", type=str, default="price_history_downloader.log",
                        help="Path to the log file.")
    parser.add_argument("--workers", type=int, default=NUM_WORKERS,
                        help=f"Number of parallel download workers (default: {NUM_WORKERS}).")

    args = parser.parse_args()

    setup_logging(args.log_file)
    logging.info("--- Starting Price History Downloader Script ---")
    logging.info(f"Arguments: {vars(args)}")

    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # --- Phase 1: Extract all market and token ID pairs ---
    market_token_pairs = extract_market_and_token_ids(args.market_details_dir)

    if not market_token_pairs:
        logging.warning("No valid (market_id, token_id) pairs extracted. Exiting.")
        exit()

    logging.info(f"Found {len(market_token_pairs)} markets with valid first CLOB token IDs.")

    # --- Phase 2: Filter IDs that need fetching ---
    pairs_to_fetch = []
    skipped_count = 0
    for market_id, clob_token_id in market_token_pairs:
        history_file_path = output_path / f"price_history_yes_{market_id}.json"
        if history_file_path.exists() and history_file_path.stat().st_size > 0:
            skipped_count += 1
        else:
            pairs_to_fetch.append((market_id, clob_token_id))

    total_pairs = len(market_token_pairs)
    needed_count = len(pairs_to_fetch)
    logging.info(f"Already downloaded: {skipped_count}. Need to fetch: {needed_count}.")

    if not pairs_to_fetch:
        logging.info("All price history files already downloaded. Exiting.")
        exit()

    # --- Phase 3: Fetch and Save details in parallel ---
    success_count = 0
    fetch_error_count = 0
    save_error_count = 0
    fetch_error_ids = []
    save_error_ids = []

    logging.info(f"Starting parallel fetching with {args.workers} workers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Map future to the market_id for easier tracking
        future_to_market_id = {
            executor.submit(fetch_and_save_price_history, market_id, token_id, args.output_dir): market_id
            for market_id, token_id in pairs_to_fetch
        }

        processed_futures = 0
        for future in concurrent.futures.as_completed(future_to_market_id):
            market_id = future_to_market_id[future]
            processed_futures += 1
            progress_percent = (processed_futures / needed_count) * 100 if needed_count > 0 else 0

            try:
                _id, status = future.result() # We know _id == market_id
                if status == "success":
                    success_count += 1
                    logging.info(f"Progress: {processed_futures}/{needed_count} ({progress_percent:.1f}%) - Success   - Market ID: {market_id}")
                elif status == "fetch_error_or_no_data":
                    fetch_error_count += 1
                    fetch_error_ids.append(market_id)
                    logging.warning(f"Progress: {processed_futures}/{needed_count} ({progress_percent:.1f}%) - Fetch Err - Market ID: {market_id}")
                elif status == "save_error":
                    save_error_count += 1
                    save_error_ids.append(market_id)
                    logging.error(f"Progress: {processed_futures}/{needed_count} ({progress_percent:.1f}%) - Save Err  - Market ID: {market_id}")
            except Exception as exc:
                fetch_error_count += 1 # Count as fetch error if worker crashes
                fetch_error_ids.append(market_id)
                logging.error(f"Market ID {market_id} generated an exception in worker: {exc}", exc_info=True) # Log traceback

    logging.info("--- Price History Downloader Script Finished ---")
    logging.info(f"Total valid market/token pairs found: {total_pairs}")
    logging.info(f"Already existed / Skipped: {skipped_count}")
    logging.info(f"Attempted to fetch: {needed_count}")
    logging.info(f"Successfully fetched & saved: {success_count}")
    logging.info(f"Fetch errors (or no data/404): {fetch_error_count}")
    logging.info(f"Save errors: {save_error_count}")
    if fetch_error_ids:
        logging.warning(f"Market IDs with fetch errors/no data (will retry on next run): {sorted(fetch_error_ids)}")
    if save_error_ids:
        logging.warning(f"Market IDs with save errors (will retry on next run): {sorted(save_error_ids)}") 