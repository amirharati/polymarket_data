"""
Processes downloaded Polymarket data.

Task 1: Reads market data JSONL files and saves each market object into an
        individual JSON file (market_{id}.json) in a specified directory.
Task 2: Reads market data JSONL files and corresponding event detail JSON files,
        joins them based on the first event listed in the market, and writes
        all fields into a single TSV file.
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

# --- Task 2: Create Joined TSV File ---
def create_joined_tsv(market_data_dir, event_details_dir, tsv_output_file):
    """Reads market and event data, joins them, and writes to a TSV file."""
    logging.info("--- Starting Task 2: Creating Joined TSV File ---")
    market_data_path = Path(market_data_dir)
    event_details_path = Path(event_details_dir)
    tsv_output_path = Path(tsv_output_file)

    if not market_data_path.is_dir():
        logging.error(f"Market data directory not found: {market_data_dir}")
        return False
    if not event_details_path.is_dir():
        logging.error(f"Event details directory not found: {event_details_dir}")
        return False

    # --- Step 2.1: Determine Headers (Dynamically, but can be pre-defined for robustness) ---
    # Let's pre-define based on observation and goals, adding prefixes.
    # This is more robust than dynamically finding all keys across all files.
    # (Add/remove fields as needed based on analysis goals)
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
        'umaReward', 'fee' # Added fee based on event_2890.json market example
    ]
    event_headers = [
        'id', 'ticker', 'slug', 'title', 'description', 'resolutionSource', 'startDate', 'creationDate',
        'endDate', 'image', 'icon', 'active', 'closed', 'archived', 'new', 'featured', 'restricted',
        'liquidity', 'volume', 'openInterest', 'sortBy', 'category', 'published_at', 'createdAt',
        'updatedAt', 'competitive', 'volume24hr', 'volume1wk', 'volume1mo', 'volume1yr',
        'enableOrderBook', 'liquidityClob', 'commentCount', 'cyom', 'closedTime', 'showAllOutcomes',
        'showMarketImages', 'enableNegRisk', 'seriesSlug', 'negRiskAugmented', 'pendingDeployment',
        'deploying'
        # Excluded: markets, series, tags (complex nested structures)
        # Placeholder for actual resolution if found in event data
        # 'resolution' # We need to confirm this field exists and holds the final outcome
    ]

    # Combine and prefix headers
    final_headers = [MARKET_PREFIX + h for h in market_headers] + [EVENT_PREFIX + h for h in event_headers]
    logging.info(f"TSV will contain {len(final_headers)} columns.")

    # --- Step 2.2: Process Files and Write TSV ---
    processed_market_count = 0
    written_row_count = 0
    event_file_missing_count = 0
    market_parse_error_count = 0
    event_parse_error_count = 0
    no_event_in_market_count = 0

    jsonl_files = list(market_data_path.glob('markets_offset_*.jsonl'))
    logging.info(f"Found {len(jsonl_files)} market data files to process for Task 2.")

    try:
        with open(tsv_output_path, 'w', newline='', encoding='utf-8') as tsvfile:
            writer = csv.writer(tsvfile, delimiter='\t', lineterminator='\n')

            # Write Header
            writer.writerow(final_headers)

            # Process each market file
            for file_path in jsonl_files:
                logging.debug(f"Processing file for TSV: {file_path.name}")
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        for line_num, line in enumerate(f, 1):
                            processed_market_count += 1
                            market_data = None
                            event_data = None
                            first_event_id = None

                            try:
                                market_data = json.loads(line.strip())
                            except json.JSONDecodeError as e:
                                logging.error(f"Market JSON decode error in {file_path.name}, line {line_num}: {e}")
                                market_parse_error_count += 1
                                continue # Skip this market line

                            # Get the first event ID from the market data
                            market_events = market_data.get('events')
                            if isinstance(market_events, list) and len(market_events) > 0:
                                first_event = market_events[0]
                                if isinstance(first_event, dict):
                                    first_event_id = first_event.get('id')

                            if not first_event_id:
                                logging.debug(f"Market ID {market_data.get('id', '?')} in {file_path.name} has no valid first event ID. Skipping event join.")
                                no_event_in_market_count += 1
                                # Still write market data even if event is missing?
                                # For now, let's require an event ID to proceed with writing the row.
                                # If we want rows with only market data, logic needs adjustment here.
                                # continue

                            # Attempt to load corresponding event data if ID was found
                            if first_event_id:
                                event_file = event_details_path / f"event_{first_event_id}.json"
                                if event_file.is_file():
                                    try:
                                        with open(event_file, 'r', encoding='utf-8') as ef:
                                            event_data = json.load(ef)
                                    except json.JSONDecodeError as e:
                                        logging.error(f"Event JSON decode error for {event_file.name}: {e}")
                                        event_parse_error_count += 1
                                        # Proceed without event data, fields will be empty
                                    except IOError as e:
                                        logging.error(f"IOError reading event file {event_file.name}: {e}")
                                        event_parse_error_count += 1
                                        # Proceed without event data
                                else:
                                    logging.warning(f"Event file not found for event ID {first_event_id} (from market {market_data.get('id', '?')})")
                                    event_file_missing_count += 1
                                    # Proceed without event data

                            # --- Construct Row Data ---
                            row_values = []
                            for header in final_headers:
                                value = '' # Default to empty string
                                if header.startswith(MARKET_PREFIX):
                                    key = header[len(MARKET_PREFIX):]
                                    if market_data:
                                        value = market_data.get(key, '')
                                elif header.startswith(EVENT_PREFIX):
                                    key = header[len(EVENT_PREFIX):]
                                    if event_data:
                                        # Exclude complex nested structures explicitly if keys were added above
                                        if key not in ['markets', 'series', 'tags']:
                                             value = event_data.get(key, '')
                                # Sanitize the value before appending
                                row_values.append(sanitize_value(value))

                            # Write the row
                            writer.writerow(row_values)
                            written_row_count += 1

                except IOError as e:
                    logging.error(f"Could not read market file {file_path.name}: {e}")
                except Exception as e:
                     logging.error(f"Unexpected error processing market file {file_path.name} for TSV: {e}")

    except IOError as e:
        logging.error(f"Could not open or write to TSV file {tsv_output_path}: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during TSV creation: {e}")
        return False

    logging.info("--- Finished Task 2 --- ")
    logging.info(f"Processed {processed_market_count} market records.")
    logging.info(f"Wrote {written_row_count} rows to {tsv_output_path}.")
    logging.info(f"Market parse errors: {market_parse_error_count}")
    logging.info(f"Event parse errors: {event_parse_error_count}")
    logging.info(f"Event files missing: {event_file_missing_count}")
    logging.info(f"Markets with no valid first event: {no_event_in_market_count}")
    return True

# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Polymarket market and event data.")
    parser.add_argument("--market-data-dir", type=str, required=True,
                        help="Directory containing the source market data .jsonl files.")
    parser.add_argument("--event-details-dir", type=str, required=True,
                        help="Directory containing the downloaded event detail JSON files.")
    parser.add_argument("--market-output-dir", type=str, required=True,
                        help="Directory to save individual market JSON files.")
    parser.add_argument("--tsv-output-file", type=str, required=True,
                        help="Path for the output joined TSV file.")
    parser.add_argument("--log-file", type=str, default="process_data.log",
                        help="Path to the log file.")
    parser.add_argument("--skip-task1", action="store_true",
                        help="Skip Task 1 (Saving individual market JSONs).")
    parser.add_argument("--skip-task2", action="store_true",
                        help="Skip Task 2 (Creating joined TSV file).")


    args = parser.parse_args()

    setup_logging(args.log_file)
    logging.info("--- Starting Data Processing Script ---")
    logging.info(f"Arguments: {vars(args)}")

    task1_success = True
    if not args.skip_task1:
        task1_success = save_individual_markets(args.market_data_dir, args.market_output_dir)
    else:
        logging.info("Skipping Task 1 based on arguments.")

    task2_success = True
    # Optionally only run task 2 if task 1 was successful or skipped
    if not args.skip_task2:
        # if task1_success:
        task2_success = create_joined_tsv(args.market_data_dir, args.event_details_dir, args.tsv_output_file)
        # else:
        #     logging.error("Skipping Task 2 because Task 1 failed.")
        #     task2_success = False
    else:
         logging.info("Skipping Task 2 based on arguments.")


    logging.info("--- Data Processing Script Finished ---")
    if not args.skip_task1:
        logging.info(f"Task 1 (Save Market JSONs) Success: {task1_success}")
    if not args.skip_task2:
        logging.info(f"Task 2 (Create TSV) Success: {task2_success}") 