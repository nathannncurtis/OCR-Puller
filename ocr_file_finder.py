import os
import shutil
import sys
import ctypes
import filecmp
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_file_base_names(prompt_dir):
    """Get base names of files excluding .db files."""
    return [
        os.path.splitext(f)[0]
        for f in os.listdir(prompt_dir)
        if os.path.isfile(os.path.join(prompt_dir, f)) and not f.lower().endswith('.db')
    ]

def find_previous_business_day_folders(search_dirs_2025):
    """Collect all valid 'day' subfolders from the past year in the 2025 directories."""
    current_date = datetime.now()
    found_dirs = []
    with ThreadPoolExecutor() as executor:
        for days_back in range(1, 366):
            target_date = current_date - timedelta(days=days_back)
            month_part = target_date.strftime("%m-%Y")
            day_part = target_date.strftime("%m_%d")
            check_paths = [os.path.join(base_dir, month_part, day_part) for base_dir in search_dirs_2025]
            futures = {executor.submit(os.path.isdir, path): path for path in check_paths}
            for fut in as_completed(futures):
                p = futures[fut]
                if fut.result():
                    found_dirs.append(p)
    return found_dirs

def get_month_folders(search_dirs, target_month, exclude_dirs):
    """Get all day folders for a specific month (MM-YYYY format)."""
    folders = []
    for base_dir in search_dirs:
        month_path = os.path.join(base_dir, target_month)
        if not os.path.exists(month_path):
            continue
        with os.scandir(month_path) as it:
            for entry in it:
                dir_path = os.path.join(month_path, entry.name)
                if entry.is_dir() and dir_path not in exclude_dirs:
                    folders.append(dir_path)
    return folders

def search_directory_for_matches(root_dir, target_name):
    """Recursively find all directories containing 'target_name' in their folder name."""
    matches = []
    for entry in os.scandir(root_dir):
        if entry.is_dir():
            if target_name in entry.name:
                matches.append(entry.path)
            matches.extend(search_directory_for_matches(entry.path, target_name))
    return matches

def phased_search(base_name, search_phases):
    """
    Run multiple search phases, collecting all matching folders
    without stopping at the first match.
    """
    searched_dirs = set()
    all_matches = []
    for phase in search_phases:
        phase_dirs = phase['get_dirs'](searched_dirs)
        if not phase_dirs:
            continue
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = {executor.submit(search_directory_for_matches, d, base_name): d for d in phase_dirs}
            for fut in as_completed(futures):
                result = fut.result()
                if result:
                    all_matches.extend(result)
        searched_dirs.update(phase_dirs)
    return all_matches, searched_dirs

def find_original_file(prompt_dir, base_name):
    """Find the original file with the given base name."""
    for fname in os.listdir(prompt_dir):
        if fname.startswith(base_name + '.'):
            return os.path.join(prompt_dir, fname)
    return None

def search_xray_covers_for_match(base_name, xray_covers_dir):
    """
    Search for matching files in the X-ray covers directory.
    Returns the full path of the matching file if found, None otherwise.
    """
    if not os.path.exists(xray_covers_dir):
        return None

    try:
        for file in os.listdir(xray_covers_dir):
            if os.path.isfile(os.path.join(xray_covers_dir, file)) and not file.lower().endswith('.db'):
                file_base = os.path.splitext(file)[0]
                if base_name in file_base:
                    return os.path.join(xray_covers_dir, file)
    except (OSError, PermissionError):
        pass
    return None

def copy_files_from_matching_folders(matching_folders, prompt_dir, base_name):
    """
    Find all files in matching folders and copy them to the prompt directory
    with proper naming to avoid conflicts.
    """
    if not matching_folders:
        return

    for folder in matching_folders:
        try:
            for root, dirs, files in os.walk(folder):
                for file in files:
                    if not file.lower().endswith('.db'):
                        source_file = os.path.join(root, file)

                        # Create a unique name in the destination
                        base_file, ext = os.path.splitext(file)
                        dest_file = os.path.join(prompt_dir, file)

                        # Handle naming conflicts
                        copy_num = 1
                        while os.path.exists(dest_file):
                            new_name = f"{base_file} ({copy_num}){ext}"
                            dest_file = os.path.join(prompt_dir, new_name)
                            copy_num += 1

                        shutil.copy2(source_file, dest_file)
        except (OSError, PermissionError):
            continue

def rename_original_files_to_copy(prompt_dir):
    """
    Rename all non-.db files in the prompt directory to include '- Copy' in the name.
    """
    files_to_rename = []

    # Get all files that need renaming
    for file in os.listdir(prompt_dir):
        file_path = os.path.join(prompt_dir, file)
        if os.path.isfile(file_path) and not file.lower().endswith('.db') and '- Copy' not in file:
            files_to_rename.append(file_path)

    # Rename each file
    for file_path in files_to_rename:
        filename = os.path.basename(file_path)
        base, ext = os.path.splitext(filename)
        new_filename = f"{base} - Copy{ext}"
        new_path = os.path.join(prompt_dir, new_filename)

        # Handle naming conflicts
        copy_num = 1
        while os.path.exists(new_path):
            alt_name = f"{base} - Copy ({copy_num}){ext}"
            new_path = os.path.join(prompt_dir, alt_name)
            copy_num += 1

        os.rename(file_path, new_path)

def main():
    if len(sys.argv) < 2:
        print("No directory provided.")
        sys.exit(1)

    prompt_dir = sys.argv[1]
    search_dirs_2025 = [r'\\ronsin158\ocr_processed\2025', r'\\ronsin232\ocr_processed\2025']
    search_dirs_all = [r'\\ronsin158\ocr_processed', r'\\ronsin232\ocr_processed']
    xray_covers_dir = r'\\nas-prod\Archive\X-RAYS TO UPLOAD'

    current_month = datetime.now().strftime("%m-%Y")
    prev_month = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%m-%Y")

    search_phases = [
        {'get_dirs': lambda sd: find_previous_business_day_folders(search_dirs_2025), 'name': 'previous day'},
        {'get_dirs': lambda sd: get_month_folders(search_dirs_2025, current_month, sd), 'name': 'current month'},
        {'get_dirs': lambda sd: get_month_folders(search_dirs_2025, prev_month, sd), 'name': 'previous month'},
        {'get_dirs': lambda sd: get_month_folders(search_dirs_2025, "2025", sd), 'name': 'entire year'},
        {'get_dirs': lambda sd: get_month_folders(search_dirs_all, "**", sd), 'name': 'full archive'}
    ]

    # First, check for X-ray cover matches and replace files immediately
    for base_name in get_file_base_names(prompt_dir):
        xray_match = search_xray_covers_for_match(base_name, xray_covers_dir)
        if xray_match:
            # Find the original file and replace it with X-ray version
            original_file = find_original_file(prompt_dir, base_name)
            if original_file and os.path.exists(original_file):
                # Replace original file with X-ray cover file
                shutil.move(xray_match, original_file)
                print(f"Replaced {base_name} with X-ray cover version")

    # Rename all original files to include "- Copy"
    rename_original_files_to_copy(prompt_dir)
    print("Renamed all original files to include '- Copy'")

    # Now process OCR matches with updated file list (including X-ray replacements)
    for base_name in get_file_base_names(prompt_dir):
        # Remove "- Copy" from base name for searching
        search_name = base_name.replace(" - Copy", "").split(" (")[0]  # Remove copy indicators

        # Search for OCR matches
        matches, _ = phased_search(search_name, search_phases)

        if matches:
            copy_files_from_matching_folders(matches, prompt_dir, search_name)
            print(f"Copied matches for {search_name}")
        else:
            print(f"No matches found for {search_name}")

    ctypes.windll.user32.MessageBoxW(0, "Files Processed!", "Finished", 0x00040000 | 0x00000001)
    os._exit(0)

if __name__ == "__main__":
    main()