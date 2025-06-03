import json
import os

def load_analysis_results(file_path="analysis_results.json"):
    """Loads the analysis results from a JSON file."""
    if not os.path.exists(file_path):
        print(f"Error: Analysis results file not found: {file_path}")
        print("Please run the analyze_price_data.py script first.")
        return None
    try:
        with open(file_path, 'r') as f:
            analysis_data = json.load(f)
        return analysis_data
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {file_path}.")
        return None
    except Exception as e:
        print(f"Error loading analysis results: {e}")
        return None

def apply_filters(analysis_data, criteria):
    """
    Filters the analysis data based on the given criteria.

    Args:
        analysis_data (list): A list of analysis result dictionaries.
        criteria (dict): A dictionary defining the filtering conditions.

    Returns:
        list: A list of filenames that meet the criteria.
    """
    filtered_filenames = []

    if not analysis_data:
        return filtered_filenames

    for file_summary in analysis_data:
        passes_filter = True

        # Check min_num_points
        if criteria.get("min_num_points") is not None and file_summary.get("num_points", 0) < criteria["min_num_points"]:
            passes_filter = False
        
        # Check max_num_points
        if criteria.get("max_num_points") is not None and file_summary.get("num_points", 0) > criteria["max_num_points"]:
            passes_filter = False

        # Check min_mean_price
        if criteria.get("min_mean_price") is not None:
            if file_summary.get("mean_price") is None or file_summary["mean_price"] < criteria["min_mean_price"]:
                passes_filter = False
        
        # Check max_mean_price
        if criteria.get("max_mean_price") is not None:
            if file_summary.get("mean_price") is None or file_summary["mean_price"] > criteria["max_mean_price"]:
                passes_filter = False

        # Check min_std_dev_price
        if criteria.get("min_std_dev_price") is not None:
            if file_summary.get("std_dev_price") is None or file_summary["std_dev_price"] < criteria["min_std_dev_price"]:
                passes_filter = False
        
        # Check max_std_dev_price
        if criteria.get("max_std_dev_price") is not None:
            if file_summary.get("std_dev_price") is None or file_summary["std_dev_price"] > criteria["max_std_dev_price"]:
                passes_filter = False
        
        # Check exclude_issues
        exclude_issues_list = criteria.get("exclude_issues", [])
        if exclude_issues_list:
            file_issues = file_summary.get("issues", [])
            for issue_to_exclude in exclude_issues_list:
                if issue_to_exclude in '; '.join(file_issues): # Check if the issue string is present
                    passes_filter = False
                    break
        
        # Check require_issues
        require_issues_list = criteria.get("require_issues", [])
        if require_issues_list:
            file_issues_str = '; '.join(file_summary.get("issues", []))
            for issue_to_require in require_issues_list:
                if issue_to_require not in file_issues_str:
                    passes_filter = False
                    break
        
        # Check max_irregular_delta_seconds
        # This checks if the largest gap between points exceeds the threshold
        max_delta_filter = criteria.get("max_irregular_delta_seconds")
        if max_delta_filter is not None:
            time_delta_stats = file_summary.get("time_delta_stats", {})
            max_recorded_delta = time_delta_stats.get("max_delta_seconds")
            if max_recorded_delta is None or max_recorded_delta > max_delta_filter:
                passes_filter = False

        if passes_filter:
            filtered_filenames.append(file_summary["filename"])
            
    return filtered_filenames

def main():
    analysis_file_path = "analysis_results.json"
    filtered_list_output_path = "filtered_filenames.txt"

    # --- Define your filtering criteria here ---
    filter_criteria = {
        "min_num_points": 1000,          
        "max_num_points": None,         
        "min_mean_price": 0.000001,          
        "max_mean_price": 1.0,          
        "min_std_dev_price": 0.06,      
        "max_std_dev_price": None,       
        "exclude_issues": [             
            "Invalid JSON format",
            "File not found",
            "No 'history' key found", # More general check for no history data
            "No valid price data points found"
        ],
        "require_issues": None, 
        "max_irregular_delta_seconds": 600 # Max allowed largest gap is 5 minutes (300 seconds)
    }
    # -------------------------------------------

    analysis_data = load_analysis_results(analysis_file_path)
    if analysis_data is None:
        return # Error message already printed by load_analysis_results

    print(f"Loaded {len(analysis_data)} file summaries from {analysis_file_path}.")
    print("Applying filters...")
    print(f"Filter Criteria: {json.dumps(filter_criteria, indent=2)}")

    passed_files = apply_filters(analysis_data, filter_criteria)

    print(f"\n--- Filtered File List ({len(passed_files)} files) ---")
    if not passed_files:
        print("No files met the specified criteria.")
    else:
        for filename in passed_files:
            print(filename)
        
        try:
            with open(filtered_list_output_path, 'w') as f_out:
                for filename in passed_files:
                    f_out.write(f"{filename}\n")
            print(f"\nList of filtered filenames saved to: {filtered_list_output_path}")
        except Exception as e:
            print(f"\nError saving filtered file list: {e}")

if __name__ == "__main__":
    main() 