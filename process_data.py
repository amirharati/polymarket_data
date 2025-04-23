"""
Processes downloaded Polymarket data.

Task 1: Reads market data JSONL files and saves each market object into an
        individual JSON file (market_{id}.json) in a specified directory.
Task 2: Reads market data JSONL files and corresponding event detail JSON files,
        joins them based on the first event listed in the market, and writes
        all fields into a single TSV file.
Task 3: Reads downloaded price history JSON files (price_history_yes_{id}.json)
        and creates a TSV file (timeseries_{id}.tsv) for each market with
        non-empty history, containing timestamp and price columns.
"""

import json
import csv
import logging
import argparse
import os
from pathlib import Path

# --- Constants ---
# Prefixes to avoid column name collisions in TSV
MARKET_PREFIX = "market_"
EVENT_PREFIX = "event_"
TIMESERIES_PREFIX = "timeseries_"

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

def sanitize_value(value):
    """Converts value to string and replaces TSV-breaking characters."""
    if value is None:
        return '' # Represent None as empty string in TSV
    s_value = str(value) # Ensure it's a string
    # Replace tabs, newlines, carriage returns with spaces
    s_value = s_value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    return s_value

# --- Task 1: Save Individual Market JSONs ---
def save_individual_markets(market_data_dir, market_output_dir):
    """Reads market JSONL files and saves each market into its own JSON file."""
    logging.info("--- Starting Task 1: Saving Individual Market JSONs ---")
    market_data_path = Path(market_data_dir)
    market_output_path = Path(market_output_dir)
    market_output_path.mkdir(parents=True, exist_ok=True)

    if not market_data_path.is_dir():
        logging.error(f"Market data directory not found: {market_data_dir}")
        return False

    jsonl_files = list(market_data_path.glob('markets_offset_*.jsonl'))
    logging.info(f"Found {len(jsonl_files)} market data files to process for Task 1.")

    processed_count = 0
    error_count = 0

    for file_path in jsonl_files:
        logging.debug(f"Processing file for Task 1: {file_path.name}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        market_data = json.loads(line.strip())
                        market_id = market_data.get('id')

                        if not market_id:
                            logging.warning(f"Skipping line {line_num} in {file_path.name}: Missing market ID.")
                            error_count += 1
                            continue

                        output_filename = market_output_path / f"market_{market_id}.json"
                        # Save the whole market object
                        with open(output_filename, 'w', encoding='utf-8') as out_f:
                            json.dump(market_data, out_f, ensure_ascii=False, indent=4)
                        processed_count += 1

                    except json.JSONDecodeError as e:
                        logging.error(f"JSON decode error in {file_path.name}, line {line_num} (Task 1): {e}")
                        error_count += 1
                    except IOError as e:
                         logging.error(f"IO error writing {output_filename} (Task 1): {e}")
                         error_count += 1
                         # Optional: break if write error is critical
                    except Exception as e:
                        logging.error(f"Unexpected error on line {line_num} in {file_path.name} (Task 1): {e}")
                        error_count += 1

        except IOError as e:
            logging.error(f"Could not read file {file_path.name} (Task 1): {e}")
            error_count += 1
        except Exception as e:
            logging.error(f"Unexpected error processing file {file_path.name} (Task 1): {e}")
            error_count += 1

    logging.info(f"--- Finished Task 1 --- ")
    logging.info(f"Successfully saved {processed_count} individual market JSON files.")
    logging.info(f"Encountered {error_count} errors during Task 1.")
    return error_count == 0 # Return True if successful

# --- Task 2: Create Market and Event TSV Files ---
def create_market_and_event_tsvs(market_data_dir, event_details_dir, price_history_dir, market_tsv_output, event_tsv_output):
    """
    Reads market JSONL, event JSON, and checks price history data.
    Writes market data to market_tsv_output (one row per market, with comma-separated
    event IDs and price history indicator).
    Writes unique event data to event_tsv_output (one row per unique event).
    """
    logging.info("--- Starting Task 2: Creating Market and Event TSV Files ---")
    market_data_path = Path(market_data_dir)
    event_details_path = Path(event_details_dir)
    price_history_path = Path(price_history_dir)
    market_tsv_path = Path(market_tsv_output)
    event_tsv_path = Path(event_tsv_output)

    if not market_data_path.is_dir():
        logging.error(f"Market data directory not found: {market_data_dir}")
        return False
    if not event_details_path.is_dir():
        logging.error(f"Event details directory not found: {event_details_dir}")
        return False
    if not price_history_path.is_dir():
        logging.error(f"Price history directory not found: {price_history_dir}")
        return False

    # --- Step 2.1: Define Headers ---
    # Keep prefixes for clarity, especially if analyzing raw files later
    market_headers = [
        'id', 'question', 'conditionId', 'slug', 'resolutionSource', 'endDate', 'category',
        'liquidity', 'startDate', 'image', 'icon', 'description', 'outcomes', 'outcomePrices',
        'volume', 'active', 'marketType', 'closed', 'marketMakerAddress', 'createdAt', 'updatedAt',
        'closedTime', 'new', 'featured', 'archived', 'restricted', 'volumeNum', 'liquidityNum',
        'endDateIso', 'startDateIso', 'hasReviewedDates', 'volume24hr', 'volume1wk', 'volume1mo',
        'volume1yr', 'clobTokenIds', 'fpmmLive', 'volumeClob', 'liquidityClob', 'creator',
        'ready', 'funded', 'cyom', 'competitive', 'approved', 'rewardsMinSize', 'rewardsMaxSpread',
        'spread', 'oneDayPriceChange', 'oneHourPriceChange', 'oneWeekPriceChange', 'oneMonthPriceChange',
        'oneYearPriceChange', 'lastTradePrice', 'bestBid', 'bestAsk', 'clearBookOnStart',
        'manualActivation', 'negRiskOther', 'umaResolutionStatuses', 'pendingDeployment', 'deploying',
        'enableOrderBook', 'orderPriceMinTickSize', 'orderMinSize', 'acceptingOrders', 'umaBond',
        'umaReward', 'fee'
    ]
    # Add the crucial event IDs column and price history indicator
    market_headers_with_ids = [MARKET_PREFIX + h for h in market_headers] + \
                                [MARKET_PREFIX + 'event_ids', MARKET_PREFIX + 'downloaded_pricehistory_nonempty']

    event_headers = [
        'id', 'ticker', 'slug', 'title', 'description', 'resolutionSource', 'startDate', 'creationDate',
        'endDate', 'image', 'icon', 'active', 'closed', 'archived', 'new', 'featured', 'restricted',
        'liquidity', 'volume', 'openInterest', 'sortBy', 'category', 'published_at', 'createdAt',
        'updatedAt', 'competitive', 'volume24hr', 'volume1wk', 'volume1mo', 'volume1yr',
        'enableOrderBook', 'liquidityClob', 'commentCount', 'cyom', 'closedTime', 'showAllOutcomes',
        'showMarketImages', 'enableNegRisk', 'seriesSlug', 'negRiskAugmented', 'pendingDeployment',
        'deploying'
        # Excluded: markets, series, tags (complex nested structures)
    ]
    event_headers_prefixed = [EVENT_PREFIX + h for h in event_headers]

    logging.info(f"Market TSV will contain {len(market_headers_with_ids)} columns.")
    logging.info(f"Event TSV will contain {len(event_headers_prefixed)} columns.")

    # --- Step 2.2: Process Files and Write TSVs ---
    processed_market_count = 0
    written_market_rows = 0
    written_event_rows = 0
    event_file_missing_count = 0
    market_parse_error_count = 0
    event_parse_error_count = 0
    processed_event_ids = set() # To track unique events written
    price_history_check_errors = 0

    jsonl_files = list(market_data_path.glob('markets_offset_*.jsonl'))
    logging.info(f"Found {len(jsonl_files)} market data files to process for Task 2.")

    try:
        # Open both files for writing
        with open(market_tsv_path, 'w', newline='', encoding='utf-8') as market_tsvfile, \
             open(event_tsv_path, 'w', newline='', encoding='utf-8') as event_tsvfile:

            market_writer = csv.writer(market_tsvfile, delimiter='\t', lineterminator='\n')
            event_writer = csv.writer(event_tsvfile, delimiter='\t', lineterminator='\n')

            # Write Headers
            market_writer.writerow(market_headers_with_ids)
            event_writer.writerow(event_headers_prefixed)

            # Process each market file
            for file_path in jsonl_files:
                logging.debug(f"Processing file for TSVs: {file_path.name}")
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        for line_num, line in enumerate(f, 1):
                            processed_market_count += 1
                            market_data = None
                            current_event_ids = []
                            event_ids_str = ""

                            try:
                                market_data = json.loads(line.strip())
                            except json.JSONDecodeError as e:
                                logging.error(f"Market JSON decode error in {file_path.name}, line {line_num}: {e}")
                                market_parse_error_count += 1
                                continue # Skip this market line

                            market_id = str(market_data.get('id', '')) if market_data else ''

                            # --- Process Market Row ---
                            market_events = market_data.get('events', []) if market_data else []
                            if isinstance(market_events, list):
                                for event in market_events:
                                    if isinstance(event, dict) and 'id' in event:
                                        current_event_ids.append(str(event['id'])) # Ensure IDs are strings

                            # Create comma-separated string
                            event_ids_str = ",".join(current_event_ids)

                            # Check for non-empty price history file
                            has_non_empty_history = False
                            if market_id:
                                price_hist_file = price_history_path / f"price_history_yes_{market_id}.json"
                                if price_hist_file.exists():
                                    try:
                                        with open(price_hist_file, 'r', encoding='utf-8') as phf:
                                            price_data = json.load(phf)
                                            if isinstance(price_data.get('history'), list) and len(price_data['history']) > 0:
                                                has_non_empty_history = True
                                    except json.JSONDecodeError as e:
                                        logging.error(f"JSON decode error reading price history file {price_hist_file.name} for market {market_id}: {e}")
                                        price_history_check_errors += 1
                                    except IOError as e:
                                         logging.error(f"IOError reading price history file {price_hist_file.name} for market {market_id}: {e}")
                                         price_history_check_errors += 1
                                    except Exception as e:
                                        logging.error(f"Unexpected error checking price history file {price_hist_file.name} for market {market_id}: {e}")
                                        price_history_check_errors += 1
                                # else: file doesn't exist, has_non_empty_history remains False

                            # Construct market row data
                            market_row_values = []
                            for header in market_headers_with_ids:
                                value = '' # Default to empty string
                                if header == MARKET_PREFIX + 'event_ids':
                                    value = event_ids_str
                                elif header == MARKET_PREFIX + 'downloaded_pricehistory_nonempty':
                                     value = str(has_non_empty_history)
                                elif header.startswith(MARKET_PREFIX):
                                    key = header[len(MARKET_PREFIX):]
                                    if market_data:
                                        # Exclude the original 'events' list itself from being written
                                        if key != 'events':
                                            value = market_data.get(key, '')
                                market_row_values.append(sanitize_value(value))

                            # Write the market row
                            market_writer.writerow(market_row_values)
                            written_market_rows += 1

                            # --- Process Event Rows (Unique) ---
                            for event_id in current_event_ids:
                                if event_id not in processed_event_ids:
                                    event_file = event_details_path / f"event_{event_id}.json"
                                    event_data = None
                                    if event_file.is_file():
                                        try:
                                            with open(event_file, 'r', encoding='utf-8') as ef:
                                                event_data = json.load(ef)
                                        except json.JSONDecodeError as e:
                                            logging.error(f"Event JSON decode error for {event_file.name}: {e}")
                                            event_parse_error_count += 1
                                        except IOError as e:
                                            logging.error(f"IOError reading event file {event_file.name}: {e}")
                                            event_parse_error_count += 1
                                    else:
                                        logging.warning(f"Event file not found for event ID {event_id} (from market {market_data.get('id', '?')})")
                                        event_file_missing_count += 1

                                    # If event data loaded successfully, write it
                                    if event_data:
                                        event_row_values = []
                                        for header in event_headers_prefixed:
                                            value = ''
                                            if header.startswith(EVENT_PREFIX):
                                                key = header[len(EVENT_PREFIX):]
                                                # Exclude complex nested structures explicitly
                                                if key not in ['markets', 'series', 'tags']:
                                                     value = event_data.get(key, '')
                                            event_row_values.append(sanitize_value(value))

                                        event_writer.writerow(event_row_values)
                                        written_event_rows += 1

                                    # Mark this event ID as processed regardless of success/failure to prevent retries
                                    processed_event_ids.add(event_id)

                except IOError as e:
                    logging.error(f"Could not read market file {file_path.name}: {e}")
                except Exception as e:
                     logging.error(f"Unexpected error processing market file {file_path.name} for TSVs: {e}")

    except IOError as e:
        logging.error(f"Could not open or write to TSV files ({market_tsv_path} / {event_tsv_path}): {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during TSV creation: {e}")
        return False

    logging.info("--- Finished Task 2 --- ")
    logging.info(f"Processed {processed_market_count} market records.")
    logging.info(f"Wrote {written_market_rows} rows to {market_tsv_path}.")
    logging.info(f"Wrote {written_event_rows} unique event rows to {event_tsv_path}.")
    logging.info(f"Market parse errors: {market_parse_error_count}")
    logging.info(f"Event parse errors (while loading for TSV): {event_parse_error_count}")
    logging.info(f"Event files missing (when referenced by market): {event_file_missing_count}")
    logging.info(f"Price history file check errors: {price_history_check_errors}")
    # Removed 'Markets with no valid first event' count as it's no longer relevant
    return True

# --- Task 3: Create Individual Timeseries TSVs ---
def create_timeseries_tsvs(price_history_dir, timeseries_output_dir):
    """
    Reads price history JSON files and creates TSV for each non-empty history.
    """
    logging.info("--- Starting Task 3: Creating Individual Timeseries TSV Files ---")
    price_history_path = Path(price_history_dir)
    timeseries_output_path = Path(timeseries_output_dir)
    timeseries_output_path.mkdir(parents=True, exist_ok=True)

    if not price_history_path.is_dir():
        logging.error(f"Price history directory not found: {price_history_dir}")
        return False

    history_files = list(price_history_path.glob('price_history_yes_*.json'))
    logging.info(f"Found {len(history_files)} price history files to process for Task 3.")

    processed_count = 0
    written_count = 0
    skipped_empty_count = 0
    error_count = 0

    for file_path in history_files:
        processed_count += 1
        market_id = file_path.stem.replace('price_history_yes_', '')
        output_filename = timeseries_output_path / f"timeseries_{market_id}.tsv"

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                price_data = json.load(f)
                history_list = price_data.get('history')

                # Check if history is a non-empty list
                if isinstance(history_list, list) and len(history_list) > 0:
                    try:
                        with open(output_filename, 'w', newline='', encoding='utf-8') as tsf:
                            writer = csv.writer(tsf, delimiter='\t', lineterminator='\n')
                            writer.writerow(['timestamp', 'price']) # Write header
                            for item in history_list:
                                # Ensure item is a dict with 't' and 'p'
                                if isinstance(item, dict) and 't' in item and 'p' in item:
                                    writer.writerow([sanitize_value(item['t']), sanitize_value(item['p'])])
                                else:
                                    logging.warning(f"Skipping invalid history item in {file_path.name}: {item}")
                        written_count += 1
                        logging.debug(f"Successfully wrote timeseries TSV: {output_filename.name}")
                    except IOError as e:
                         logging.error(f"IO error writing {output_filename.name}: {e}")
                         error_count += 1
                else:
                    logging.debug(f"Skipping empty or invalid history in {file_path.name}")
                    skipped_empty_count += 1

        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error in {file_path.name} (Task 3): {e}")
            error_count += 1
        except IOError as e:
            logging.error(f"Could not read file {file_path.name} (Task 3): {e}")
            error_count += 1
        except Exception as e:
            logging.error(f"Unexpected error processing file {file_path.name} (Task 3): {e}")
            error_count += 1

    logging.info(f"--- Finished Task 3 --- ")
    logging.info(f"Processed {processed_count} price history files.")
    logging.info(f"Successfully wrote {written_count} non-empty timeseries TSV files to {timeseries_output_dir}.")
    logging.info(f"Skipped {skipped_empty_count} files due to empty or invalid history.")
    logging.info(f"Encountered {error_count} errors during Task 3.")
    return error_count == 0 # Return True if successful

# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Polymarket market and event data.")
    parser.add_argument("--market-data-dir", type=str, required=True,
                        help="Directory containing the source market data .jsonl files.")
    parser.add_argument("--event-details-dir", type=str, required=True,
                        help="Directory containing the downloaded event detail JSON files.")
    parser.add_argument("--price-history-dir", type=str,
                        help="Directory containing downloaded price history JSON files (price_history_yes_*.json). Required if not skipping Task 2 or Task 3.")
    parser.add_argument("--market-output-dir", type=str,
                        help="Directory to save individual market JSON files (Task 1). Only required if not skipping Task 1.")
    parser.add_argument("--market-tsv-output", type=str,
                        help="Path for the output market data TSV file (Task 2). Only required if not skipping Task 2.")
    parser.add_argument("--event-tsv-output", type=str,
                        help="Path for the output unique event data TSV file (Task 2). Only required if not skipping Task 2.")
    parser.add_argument("--timeseries-output-dir", type=str,
                        help="Directory to save individual timeseries TSV files (Task 3). Only required if not skipping Task 3.")
    parser.add_argument("--log-file", type=str, default="process_data.log",
                        help="Path to the log file.")
    parser.add_argument("--skip-task1", action="store_true",
                        help="Skip Task 1 (Saving individual market JSONs).")
    parser.add_argument("--skip-task2", action="store_true",
                        help="Skip Task 2 (Creating market and event TSV files).")
    parser.add_argument("--skip-task3", action="store_true",
                        help="Skip Task 3 (Creating individual timeseries TSV files).")


    args = parser.parse_args()

    # Validate arguments based on skipped tasks
    if not args.skip_task1 and not args.market_output_dir:
        parser.error("--market-output-dir is required when Task 1 is not skipped.")
    if not args.skip_task2:
        if not args.market_tsv_output or not args.event_tsv_output:
            parser.error("--market-tsv-output and --event-tsv-output are required when Task 2 is not skipped.")
        if not args.price_history_dir:
             parser.error("--price-history-dir is required when Task 2 is not skipped (for checking history existence).")
    if not args.skip_task3:
        if not args.price_history_dir:
             parser.error("--price-history-dir is required when Task 3 is not skipped.")
        if not args.timeseries_output_dir:
             parser.error("--timeseries-output-dir is required when Task 3 is not skipped.")


    setup_logging(args.log_file)
    logging.info("--- Starting Data Processing Script ---")
    logging.info(f"Arguments: {vars(args)}")

    task1_success = True
    if not args.skip_task1:
        task1_success = save_individual_markets(args.market_data_dir, args.market_output_dir)
    else:
        logging.info("Skipping Task 1 based on arguments.")

    task2_success = True
    # Only run task 2 if it's not skipped
    if not args.skip_task2:
        task2_success = create_market_and_event_tsvs(
            args.market_data_dir,
            args.event_details_dir,
            args.price_history_dir,
            args.market_tsv_output,
            args.event_tsv_output
        )
    else:
         logging.info("Skipping Task 2 based on arguments.")

    task3_success = True
    if not args.skip_task3:
         task3_success = create_timeseries_tsvs(
            args.price_history_dir,
            args.timeseries_output_dir
         )
    else:
         logging.info("Skipping Task 3 based on arguments.")


    logging.info("--- Data Processing Script Finished ---")
    if not args.skip_task1:
        logging.info(f"Task 1 (Save Market JSONs) Success: {task1_success}")
    if not args.skip_task2:
        logging.info(f"Task 2 (Create Market/Event TSVs) Success: {task2_success}")
    if not args.skip_task3:
         logging.info(f"Task 3 (Create Timeseries TSVs) Success: {task3_success}") 