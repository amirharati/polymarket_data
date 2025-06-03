import os
import json
import glob
import statistics
from datetime import datetime
import multiprocessing

def analyze_file(file_path):
    """
    Analyzes a single price history JSON file.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: A dictionary containing analysis results (filename, num_points, 
              mean_price, std_dev_price, issues).
              Returns None if the file is invalid or cannot be processed.
    """
    results = {
        "filename": os.path.basename(file_path),
        "num_points": 0,
        "mean_price": None,
        "std_dev_price": None,
        "min_time": None,
        "max_time": None,
        "time_delta_stats": {},
        "issues": []
    }

    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        results["issues"].append(f"File not found: {file_path}")
        return results
    except json.JSONDecodeError:
        results["issues"].append(f"Invalid JSON format: {file_path}")
        return results
    except Exception as e:
        results["issues"].append(f"Error processing file {file_path}: {str(e)}")
        return results

    history = data.get("history")
    if not isinstance(history, list):
        results["issues"].append("No 'history' key found, history is not a list, or history is empty.")
        results["num_points"] = 0
        return results
    
    if not history:
        results["issues"].append("History list is empty.")
        results["num_points"] = 0
        return results

    prices = []
    timestamps = []
    malformed_points_count = 0
    for point in history:
        if isinstance(point, dict) and "p" in point and "t" in point:
            try:
                price = float(point["p"])
                timestamp = int(point["t"])
                prices.append(price)
                timestamps.append(timestamp)
            except (ValueError, TypeError):
                malformed_points_count +=1
        else:
            malformed_points_count += 1
    
    if malformed_points_count > 0:
        results["issues"].append(f"{malformed_points_count} malformed data point(s) found and skipped.")

    results["num_points"] = len(prices)

    if not prices:
        results["issues"].append("No valid price data points found after parsing.")
        return results

    results["mean_price"] = statistics.mean(prices)

    if results["num_points"] < 2:
        results["issues"].append("Not enough data points (need at least 2) to calculate standard deviation.")
    else:
        results["std_dev_price"] = statistics.stdev(prices)
        if results["std_dev_price"] == 0:
            results["issues"].append("Price is constant throughout the file (StdDev is 0).")
    
    if timestamps:
        try:
            results["min_time"] = datetime.fromtimestamp(min(timestamps)).isoformat()
            results["max_time"] = datetime.fromtimestamp(max(timestamps)).isoformat()
        except Exception as e:
            results["issues"].append(f"Error processing timestamps for time range: {str(e)}")
        
        time_deltas = []
        if len(timestamps) > 1:
            for i in range(len(timestamps) - 1):
                time_deltas.append(timestamps[i+1] - timestamps[i])
        
        if time_deltas:
            try:
                results["time_delta_stats"] = {
                    "min_delta_seconds": min(time_deltas),
                    "max_delta_seconds": max(time_deltas),
                    "mean_delta_seconds": round(statistics.mean(time_deltas), 2),
                    "median_delta_seconds": statistics.median(time_deltas),
                    "num_deltas": len(time_deltas)
                }
                delta_counts = {}
                for delta in time_deltas:
                    delta_counts[delta] = delta_counts.get(delta, 0) + 1
                
                non_60_deltas = {d: c for d, c in delta_counts.items() if d != 60}
                if non_60_deltas:
                    results["time_delta_stats"]["non_60_second_deltas"] = non_60_deltas
            except Exception as e:
                results["issues"].append(f"Error calculating time delta statistics: {str(e)}")

    if prices and results["num_points"] < 5:
        results["issues"].append(f"Very few data points ({results['num_points']}).")

    return results

def main():
    price_data_folder = "price_history"
    output_file_path = "analysis_summary.txt"
    
    if not os.path.isdir(price_data_folder):
        print(f"Error: Directory '{price_data_folder}' not found in the current workspace: {os.getcwd()}")
        print("Please ensure the script is run from the workspace root or specify the correct path.")
        return

    json_files = glob.glob(os.path.join(price_data_folder, "*.json"))

    if not json_files:
        print(f"No JSON files found in '{price_data_folder}'.")
        return

    print(f"Found {len(json_files)} JSON files in '{price_data_folder}'. Processing with multiprocessing...")
    
    all_results = []
    with multiprocessing.Pool() as pool:
        try:
            all_results = pool.map(analyze_file, json_files)
        except Exception as e:
            print(f"An error occurred during parallel processing: {e}")
            pass

    all_results = [res for res in all_results if res is not None]

    print("\n--- Analysis Summary ---")
    
    # For calculating global stats
    valid_means = []
    valid_std_devs = []
    all_num_points = [] # Added for num_points stats

    with open(output_file_path, 'w') as f_out:
        f_out.write("Price History Analysis Summary\n")
        f_out.write("=============================\n\n")
        
        processed_count = 0
        error_files_count = 0
        empty_files_count = 0
        constant_price_files = 0
        low_data_point_files = 0

        for result in all_results:
            processed_count += 1
            f_out.write(f"File: {result['filename']}\n")
            f_out.write(f"  Number of Data Points: {result['num_points']}\n")
            all_num_points.append(result['num_points']) # Collect for global stats
            
            is_empty_or_no_data = "No 'history' key found" in '; '.join(result['issues']) or \
                                  "History list is empty." in '; '.join(result['issues']) or \
                                  "No valid price data points found" in '; '.join(result['issues'])
            if is_empty_or_no_data:
                 empty_files_count +=1

            if result["mean_price"] is not None:
                f_out.write(f"  Mean Price: {result['mean_price']:.4f}\n")
                valid_means.append(result['mean_price']) # Collect for global stats
            else:
                f_out.write("  Mean Price: N/A\n")

            if result["std_dev_price"] is not None:
                f_out.write(f"  Std Dev Price: {result['std_dev_price']:.4f}\n")
                valid_std_devs.append(result['std_dev_price']) # Collect for global stats
                if result["std_dev_price"] == 0 and result["num_points"] >= 2:
                    constant_price_files +=1
            else:
                 f_out.write("  Std Dev Price: N/A\n")

            if result["min_time"] and result["max_time"]:
                f_out.write(f"  Time Range: {result['min_time']} to {result['max_time']}\n")
            else:
                f_out.write("  Time Range: N/A\n")
            
            if result["time_delta_stats"]:
                td_stats = result['time_delta_stats']
                mean_delta_str = f"{td_stats.get('mean_delta_seconds', 'N/A'):.2f}" if isinstance(td_stats.get('mean_delta_seconds'), (int, float)) else 'N/A'
                median_delta_str = f"{td_stats.get('median_delta_seconds', 'N/A')}" if isinstance(td_stats.get('median_delta_seconds'), (int, float)) else 'N/A'
                f_out.write(f"  Timestamp Differences (seconds):\n")
                f_out.write(f"    Min: {td_stats.get('min_delta_seconds', 'N/A')}, Max: {td_stats.get('max_delta_seconds', 'N/A')}, Mean: {mean_delta_str}, Median: {median_delta_str}\n")
                if "non_60_second_deltas" in td_stats:
                    f_out.write(f"    Irregular deltas (delta: count): {td_stats['non_60_second_deltas']}\n")
            else:
                f_out.write("  Timestamp Differences (seconds): N/A (Not enough data or error)\n")

            if f"Very few data points" in '; '.join(result['issues']):
                low_data_point_files +=1

            if result["issues"]:
                f_out.write(f"  Issues: {'; '.join(result['issues'])}\n")
                if any("File not found" in issue or "Invalid JSON" in issue or "Error processing file" in issue for issue in result['issues']):
                    error_files_count +=1
            else:
                f_out.write("  Issues: None\n")
            f_out.write("-" * 30 + "\n")
            
            print(f"\nSummary for: {result['filename']}")
            print(f"  Points: {result['num_points']}, Mean: {result['mean_price'] if result['mean_price'] is not None else 'N/A'}, StdDev: {result['std_dev_price'] if result['std_dev_price'] is not None else 'N/A'}")
            if result["issues"]:
                print(f"  Issues: {'; '.join(result['issues'])}")
        
        # Calculate global statistics
        global_average_mean_price = statistics.mean(valid_means) if valid_means else None
        global_std_dev_of_means = statistics.stdev(valid_means) if len(valid_means) >= 2 else None
        global_average_std_dev = statistics.mean(valid_std_devs) if valid_std_devs else None
        global_std_dev_of_std_devs = statistics.stdev(valid_std_devs) if len(valid_std_devs) >= 2 else None

        # Global stats for number of points
        global_min_num_points = min(all_num_points) if all_num_points else None
        global_max_num_points = max(all_num_points) if all_num_points else None
        global_average_num_points = statistics.mean(all_num_points) if all_num_points else None
        global_median_num_points = statistics.median(all_num_points) if all_num_points else None
        global_std_dev_of_num_points = statistics.stdev(all_num_points) if len(all_num_points) >= 2 else None

        f_out.write("\nOverall Statistics\n")
        f_out.write("==================\n")
        f_out.write(f"Total files found: {len(json_files)}\n")
        f_out.write(f"Total files processed: {processed_count}\n")
        f_out.write(f"Files with read/parse errors: {error_files_count}\n")
        f_out.write(f"Files with no history/data points: {empty_files_count}\n")
        f_out.write(f"Files with constant price: {constant_price_files}\n")
        f_out.write(f"Files with very few data points (<5): {low_data_point_files}\n\n")
        
        f_out.write("Global Data Characteristics (across all processed files):\n") # Renamed section
        f_out.write("  Number of Points:\n")
        f_out.write(f"    Min: {global_min_num_points if global_min_num_points is not None else 'N/A'}\n")
        f_out.write(f"    Max: {global_max_num_points if global_max_num_points is not None else 'N/A'}\n")
        f_out.write(f"    Mean: {f'{global_average_num_points:.2f}' if global_average_num_points is not None else 'N/A'}\n")
        f_out.write(f"    Median: {global_median_num_points if global_median_num_points is not None else 'N/A'}\n")
        f_out.write(f"    Std Dev: {f'{global_std_dev_of_num_points:.2f}' if global_std_dev_of_num_points is not None else 'N/A'}\n")
        f_out.write("  Mean Prices (of files with valid means):\n") # Clarified scope
        f_out.write(f"    Average of Means: {f'{global_average_mean_price:.4f}' if global_average_mean_price is not None else 'N/A'}\n")
        f_out.write(f"    Std Dev of Means: {f'{global_std_dev_of_means:.4f}' if global_std_dev_of_means is not None else 'N/A'}\n")
        f_out.write("  Standard Deviations (of files with valid std devs):\n") # Clarified scope
        f_out.write(f"    Average of Std Devs: {f'{global_average_std_dev:.4f}' if global_average_std_dev is not None else 'N/A'}\n")
        f_out.write(f"    Std Dev of Std Devs: {f'{global_std_dev_of_std_devs:.4f}' if global_std_dev_of_std_devs is not None else 'N/A'}\n")

    # Save all_results to a JSON file for other scripts to use
    results_output_path = "analysis_results.json"
    try:
        with open(results_output_path, 'w') as json_f_out:
            json.dump(all_results, json_f_out, indent=4)
        print(f"\nFull analysis results saved to {results_output_path}")
    except Exception as e:
        print(f"\nError saving analysis results to JSON: {e}")

    print(f"\nDetailed summary written to {output_file_path}")
    print(f"Overall: {len(json_files)} found, {processed_count} processed. Errors: {error_files_count}, Empty: {empty_files_count}, Constant: {constant_price_files}, Low Data: {low_data_point_files}")
    print("Global Data Characteristics (across all processed files):") # Renamed section
    print("  Number of Points:")
    print(f"    Min: {global_min_num_points if global_min_num_points is not None else 'N/A'}")
    print(f"    Max: {global_max_num_points if global_max_num_points is not None else 'N/A'}")
    print(f"    Mean: {f'{global_average_num_points:.2f}' if global_average_num_points is not None else 'N/A'}")
    print(f"    Median: {global_median_num_points if global_median_num_points is not None else 'N/A'}")
    print(f"    Std Dev: {f'{global_std_dev_of_num_points:.2f}' if global_std_dev_of_num_points is not None else 'N/A'}")
    print("  Mean Prices (of files with valid means):") # Clarified scope
    print(f"    Average of Means: {f'{global_average_mean_price:.4f}' if global_average_mean_price is not None else 'N/A'}")
    print(f"    Std Dev of Means: {f'{global_std_dev_of_means:.4f}' if global_std_dev_of_means is not None else 'N/A'}")
    print("  Standard Deviations (of files with valid std devs):") # Clarified scope
    print(f"    Average of Std Devs: {f'{global_average_std_dev:.4f}' if global_average_std_dev is not None else 'N/A'}")
    print(f"    Std Dev of Std Devs: {f'{global_std_dev_of_std_devs:.4f}' if global_std_dev_of_std_devs is not None else 'N/A'}")

if __name__ == "__main__":
    main() 