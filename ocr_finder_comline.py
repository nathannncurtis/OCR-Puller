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

def copy_folder_with_contents(src_folder, dest_dir):
    """
    Copy the entire folder structure to dest_dir, handling naming conflicts.
    Returns the path to the copied folder.
    """
    folder_name = os.path.basename(src_folder)
    dest_folder = os.path.join(dest_dir, folder_name)
    
    # Handle naming conflicts
    if os.path.exists(dest_folder):
        copy_number = 1
        while True:
            temp_name = f"{folder_name} - Copy ({copy_number})"
            attempt = os.path.join(dest_dir, temp_name)
            if not os.path.exists(attempt):
                dest_folder = attempt
                break
            copy_number += 1

    os.makedirs(dest_folder, exist_ok=True)

    # Copy all contents except .db files
    for root, dirs, files in os.walk(src_folder):
        for d in dirs:
            rel_path = os.path.relpath(os.path.join(root, d), src_folder)
            os.makedirs(os.path.join(dest_folder, rel_path), exist_ok=True)
        for f in files:
            if not f.lower().endswith('.db'):
                rel_root = os.path.relpath(root, src_folder)
                subdir = os.path.join(dest_folder, rel_root)
                os.makedirs(subdir, exist_ok=True)
                shutil.copy2(os.path.join(root, f), os.path.join(subdir, f))

    return dest_folder

def find_original_file(prompt_dir, base_name):
    """Find the original file with the given base name."""
    for fname in os.listdir(prompt_dir):
        if fname.startswith(base_name + '.'):
            return os.path.join(prompt_dir, fname)
    return None

def rename_and_move_original(original_file_path, dest_folder):
    """
    Rename the original file to add '- Copy' and move it into the destination folder.
    """
    if not original_file_path or not os.path.exists(original_file_path):
        return
    
    filename = os.path.basename(original_file_path)
    base, ext = os.path.splitext(filename)
    new_filename = f"{base} - Copy{ext}"
    
    dest_path = os.path.join(dest_folder, new_filename)
    
    # Handle naming conflicts in destination
    copy_num = 1
    while os.path.exists(dest_path):
        alt_name = f"{base} - Copy ({copy_num}){ext}"
        dest_path = os.path.join(dest_folder, alt_name)
        copy_num += 1
    
    shutil.move(original_file_path, dest_path)

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

def copy_matching_contents(matching_folders, prompt_dir, base_name):
    """
    1) Copy each matching folder to the prompt directory
    2) Find the original file and rename it to include '- Copy'
    3) Move the renamed original file into each copied folder
    """
    if not matching_folders:
        return

    # Find the original file
    original_file_path = find_original_file(prompt_dir, base_name)
    if not original_file_path:
        return

    copied_folders = []
    
    # Copy all matching folders
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(copy_folder_with_contents, folder, prompt_dir): folder for folder in matching_folders}
        for fut in as_completed(futures):
            copied_folder = fut.result()
            copied_folders.append(copied_folder)

    # For each copied folder, copy the renamed original file into it
    filename = os.path.basename(original_file_path)
    base, ext = os.path.splitext(filename)
    new_filename = f"{base} - Copy{ext}"
    
    for copied_folder in copied_folders:
        dest_path = os.path.join(copied_folder, new_filename)
        
        # Handle naming conflicts
        copy_num = 1
        while os.path.exists(dest_path):
            alt_name = f"{base} - Copy ({copy_num}){ext}"
            dest_path = os.path.join(copied_folder, alt_name)
            copy_num += 1
        
        shutil.copy2(original_file_path, dest_path)
    
    # Remove the original file after copying it to all folders
    os.remove(original_file_path)

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

    # Now process OCR matches with updated file list (including X-ray replacements)
    for base_name in get_file_base_names(prompt_dir):
        # Search for OCR matches
        matches, _ = phased_search(base_name, search_phases)
        
        if matches:
            copy_matching_contents(matches, prompt_dir, base_name)
            print(f"Copied matches for {base_name}")
        else:
            # If absolutely no matches, move the base file(s) into "NOT IN OCR"
            unable_dir = os.path.join(prompt_dir, "NOT IN OCR")
            os.makedirs(unable_dir, exist_ok=True)
            for f in os.listdir(prompt_dir):
                if f.startswith(base_name + '.'):
                    shutil.move(os.path.join(prompt_dir, f), unable_dir)
            print(f"No matches found for {base_name}")

    ctypes.windll.user32.MessageBoxW(0, "Records Copied!", "Finished", 0x00040000 | 0x00000001)
    os._exit(0)

if __name__ == "__main__":
    main()