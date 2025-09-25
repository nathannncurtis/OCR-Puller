import os
import shutil
import sys
import ctypes
import filecmp
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_file_base_names(prompt_dir):
    """Get base names of files excluding .db files."""
    files = [
        os.path.splitext(f)[0]
        for f in os.listdir(prompt_dir)
        if os.path.isfile(os.path.join(prompt_dir, f)) and not f.lower().endswith('.db')
    ]
    print(f"Found {len(files)} files to process")
    return files

def find_incremental_business_day_folders(search_dirs_2025, base_name):
    """Search incrementally - yesterday first, then expand if no matches found."""
    current_date = datetime.now()

    # First check last 7 days one by one, stopping as soon as matches are found
    for days_back in range(1, 8):
        target_date = current_date - timedelta(days=days_back)
        day_name = target_date.strftime("%A")

        month_part = target_date.strftime("%m-%Y")
        day_part = target_date.strftime("%m_%d")

        # Check each server directory for this day
        day_dirs = []
        for base_dir in search_dirs_2025:
            check_path = os.path.join(base_dir, month_part, day_part)
            if os.path.isdir(check_path):
                day_dirs.append(check_path)

        if day_dirs:
            # Search for matches in this day's directories
            matches = []
            with ThreadPoolExecutor(max_workers=len(day_dirs)) as executor:
                futures = {executor.submit(search_directory_for_matches, d, base_name): d for d in day_dirs}

                # Process results as they come in - stop as soon as we find any matches
                for fut in as_completed(futures):
                    result = fut.result()
                    if result:
                        matches.extend(result)
                        # Cancel remaining futures since we found matches
                        for remaining_fut in futures:
                            if not remaining_fut.done():
                                remaining_fut.cancel()
                        break

            if matches:
                print(f"  Found {len(matches)} matches in {day_name}")
                return matches

    # If no matches in last 7 days, search rest of 2025
    print(f"  Expanding to full 2025 search...")
    all_matches = []

    # Search remaining days in 2025
    with ThreadPoolExecutor(max_workers=16) as executor:
        # Submit all directory existence checks first
        directory_futures = []
        for days_back in range(8, 366):
            target_date = current_date - timedelta(days=days_back)
            month_part = target_date.strftime("%m-%Y")
            day_part = target_date.strftime("%m_%d")
            for base_dir in search_dirs_2025:
                check_path = os.path.join(base_dir, month_part, day_part)
                directory_futures.append((executor.submit(os.path.isdir, check_path), check_path))

        # Collect existing directories
        existing_dirs = []
        for fut, path in directory_futures:
            if fut.result():
                existing_dirs.append(path)

        # Now search all existing directories for matches
        if existing_dirs:
            search_futures = {executor.submit(search_directory_for_matches, d, base_name): d for d in existing_dirs}
            for fut in as_completed(search_futures):
                result = fut.result()
                if result:
                    all_matches.extend(result)

    return all_matches

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

def incremental_search(base_name, search_dirs_2025, search_dirs_all):
    """
    Incremental search - start with recent days, expand only if no matches found.
    """
    # Phase 1: Incremental daily search (last 7 days, then full 2025 if needed)
    matches = find_incremental_business_day_folders(search_dirs_2025, base_name)
    if matches:
        return matches, set()

    # Phase 2: Current month (if not already covered)
    matches = []
    current_month = datetime.now().strftime("%m-%Y")
    month_folders = get_month_folders(search_dirs_2025, current_month, set())
    if month_folders:
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = {executor.submit(search_directory_for_matches, d, base_name): d for d in month_folders}
            for fut in as_completed(futures):
                result = fut.result()
                if result:
                    matches.extend(result)

        if matches:
            print(f"  Found matches in current month")
            return matches, set()

    # Phase 3: Previous month
    prev_month = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%m-%Y")
    prev_month_folders = get_month_folders(search_dirs_2025, prev_month, set())
    if prev_month_folders:
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = {executor.submit(search_directory_for_matches, d, base_name): d for d in prev_month_folders}
            for fut in as_completed(futures):
                result = fut.result()
                if result:
                    matches.extend(result)

        if matches:
            print(f"  Found matches in previous month")
            return matches, set()

    # Phase 4: Full archive (last resort)
    print(f"  Searching full archive...")
    archive_folders = []
    for base_dir in search_dirs_all:
        if os.path.exists(base_dir):
            archive_folders.append(base_dir)

    if archive_folders:
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = {executor.submit(search_directory_for_matches, d, base_name): d for d in archive_folders}
            for fut in as_completed(futures):
                result = fut.result()
                if result:
                    matches.extend(result)

    return matches, set()

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

    copied_count = 0

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
                        copied_count += 1
        except (OSError, PermissionError):
            continue

    return copied_count

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
    search_dirs_2025 = [r'\\ronsin158\ocr_processed\2025', r'\\192.168.30.208\ocr_processed\2025']
    search_dirs_all = [r'\\ronsin158\ocr_processed', r'\\192.168.30.208\ocr_processed']
    xray_covers_dir = r'\\nas-prod\Archive\X-RAYS TO UPLOAD'

    current_month = datetime.now().strftime("%m-%Y")
    prev_month = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%m-%Y")


    print("=" * 60)
    print("STARTING OCR FILE FINDER")
    print(f"Prompt directory: {prompt_dir}")
    print("=" * 60)

    # First, check for X-ray cover matches and replace files immediately
    print("\n--- X-RAY COVER REPLACEMENT PHASE ---")
    for base_name in get_file_base_names(prompt_dir):
        xray_match = search_xray_covers_for_match(base_name, xray_covers_dir)
        if xray_match:
            # Find the original file and replace it with X-ray version
            original_file = find_original_file(prompt_dir, base_name)
            if original_file and os.path.exists(original_file):
                # Replace original file with X-ray cover file
                shutil.move(xray_match, original_file)
                print(f"✓ Replaced {base_name} with X-ray cover version")

    # Rename all original files to include "- Copy"
    print("\n--- FILE RENAMING PHASE ---")
    rename_original_files_to_copy(prompt_dir)
    print("✓ Renamed all original files to include '- Copy'")

    # Now process OCR matches with updated file list (including X-ray replacements)
    print("\n--- OCR MATCHING PHASE ---")
    file_list = get_file_base_names(prompt_dir)

    if not file_list:
        print("DEBUG: No files to process")
        return

    # Create search tasks - assign each file to a worker before starting
    search_tasks = []
    for base_name in file_list:
        search_name = base_name.replace(" - Copy", "").split(" (")[0]  # Remove copy indicators
        search_tasks.append({
            'base_name': base_name,
            'search_name': search_name,
            'assigned_worker': len(search_tasks) % 8  # Assign to worker 0-7
        })

    print(f"Assigned {len(search_tasks)} files to 8 workers")

    # Execute searches in parallel with pre-assigned workers
    def search_worker(task):
        """Worker function that performs the incremental search for one file."""
        base_name = task['base_name']
        search_name = task['search_name']
        worker_id = task['assigned_worker']

        print(f"Worker {worker_id}: Searching {search_name}...")

        # Search for OCR matches using incremental search
        matches, _ = incremental_search(search_name, search_dirs_2025, search_dirs_all)

        return {
            'base_name': base_name,
            'search_name': search_name,
            'worker_id': worker_id,
            'matches': matches
        }

    # Run all searches in parallel
    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_task = {executor.submit(search_worker, task): task for task in search_tasks}

        for future in as_completed(future_to_task):
            result = future.result()
            results.append(result)

            # Process results as they complete
            if result['matches']:
                copied_count = copy_files_from_matching_folders(result['matches'], prompt_dir, result['search_name'])
                print(f"✓ Worker {result['worker_id']}: Found and copied {copied_count} files for {result['search_name']}")
            else:
                print(f"✗ Worker {result['worker_id']}: No matches found for {result['search_name']}")

    print(f"\nCompleted processing {len(results)} files")

    print("\n" + "=" * 60)
    print("OCR FILE FINDER COMPLETED")
    print("=" * 60)

    ctypes.windll.user32.MessageBoxW(0, "Found Records!", "Finished", 0x00040000 | 0x00000001)
    os._exit(0)

if __name__ == "__main__":
    main()