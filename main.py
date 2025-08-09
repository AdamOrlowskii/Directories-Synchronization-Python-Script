import os
import json
import shutil
import sys
import time
import hashlib
from datetime import datetime


def calculate_md5(filepath):
    hash_md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except (IOError, OSError) as e:
        print(f"Error: Could not calculate hash for {filepath}: {e}")
        return None


def get_directory_contents(path):
    contents = {}
    if not os.path.exists(path):
        return contents

    for root, dirs, files in os.walk(path):
        rel_path = os.path.relpath(root, path)

        for d in dirs:
            dir_rel_path = os.path.join(rel_path, d) if rel_path != "." else d
            contents[dir_rel_path] = {"type": "dir"}

        for f in files:
            file_rel_path = os.path.join(rel_path, f) if rel_path != "." else f
            file_full_path = os.path.join(root, f)
            contents[file_rel_path] = {
                "type": "file",
                "hash": calculate_md5(file_full_path)
            }

    return contents


def log_operation(operation, path, log_file_path):
    timestamp = datetime.now().isoformat()

    print(f"[{timestamp}] {operation}: {path}")

    log_entry = {
        "timestamp": timestamp,
        "operation": operation,
        "path": path
    }

    logs = []
    if os.path.exists(log_file_path):
        try:
            with open(log_file_path, "r") as f:
                logs = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error: Could not read existing log file: {e}. Starting fresh.")
            logs = []

    logs.append(log_entry)

    log_dir = os.path.dirname(log_file_path)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    try:
        with open(log_file_path, "w") as f:
            json.dump(logs, f, indent=2)
    except Exception as e:
        print(f"Warning: Could not write to a log file: {e}")


def create_replica_root(replica_path, log_file_path):

    if not os.path.exists(replica_path):
        os.makedirs(replica_path, exist_ok=True)
        log_operation("CREATE", replica_path, log_file_path)


def sync_directories(source_contents, replica_contents, replica_path, log_file_path):

    processed = set()

    for rel_path, info in source_contents.items():
        if info["type"] == "dir":
            full_replica_path = os.path.join(replica_path, rel_path)
            if rel_path not in replica_contents:
                os.makedirs(full_replica_path, exist_ok=True)
                log_operation("CREATE", rel_path, log_file_path)
            processed.add(rel_path)
    return processed


def sync_files(source_contents, replica_contents, source_path, replica_path, log_file_path):
    processed = set()

    for rel_path, info in source_contents.items():
        if info["type"] == "file":
            source_file = os.path.join(source_path, rel_path)
            replica_file = os.path.join(replica_path, rel_path)

            needs_copy = False
            operation = "COPY"

            if rel_path not in replica_contents:
                needs_copy = True
                operation = "COPY"
            elif replica_contents[rel_path]["hash"] != info["hash"]:
                needs_copy = True
                operation = "UPDATE"

            if needs_copy:
                parent_dir = os.path.dirname(replica_file)
                if parent_dir and not os.path.exists(parent_dir):
                    os.makedirs(parent_dir, exist_ok=True)

                shutil.copy2(source_file, replica_file)
                log_operation(operation, rel_path, log_file_path)

            processed.add(rel_path)

    return processed


def remove_item(replica_contents, replica_path, processed, log_file_path):

    for rel_path, info in replica_contents.items():
        if rel_path not in processed:
            full_replica_path = os.path.join(replica_path, rel_path)

            if info["type"] == "file":
                if os.path.exists(full_replica_path):
                    os.remove(full_replica_path)
                    log_operation("REMOVE", rel_path, log_file_path)
            elif info["type"] == "dir":
                if os.path.exists(full_replica_path):
                    try:
                        shutil.rmtree(full_replica_path)
                        log_operation("REMOVE", rel_path, log_file_path)
                    except (OSError, shutil.Error) as e:
                        print(f"Error: Could not remove directory {rel_path}: {e}")


def synchronize(source_path, replica_path, log_file_path):

    source_contents = get_directory_contents(source_path)
    replica_contents = get_directory_contents(replica_path)

    create_replica_root(replica_path, log_file_path)

    processed = set()

    processed.update(
        sync_directories(source_contents, replica_contents, replica_path, log_file_path)
    )

    processed.update(
        sync_files(source_contents, replica_contents, source_path, replica_path, log_file_path)
    )

    remove_item(replica_contents, replica_path, processed, log_file_path)


def main(source_path, replica_path, interval, amount, log_file_path):

    try:
        interval = int(interval)
        amount = int(amount)
    except ValueError:
        print("Error: Interval and amount must be integers")
        return

    if not os.path.exists(source_path):
        print(f"Error: Source path does not exist: {source_path}")
        return

    for iteration in range(amount):

        print(f"\nSynchronization: {iteration + 1}/{amount}")

        try:
            synchronize(source_path, replica_path, log_file_path)
        except Exception as e:
            print(f"Error during synchronization: {e}")
            return

        if iteration < amount - 1:
            print(f"Waiting {interval} seconds until next sync...")
            time.sleep(interval)

    print("\n" + "=" * 50)
    print("Synchronization process completed successfully!")


if __name__ == "__main__":
    args = sys.argv

    if len(args) != 6:
        raise Exception("You must pass 5 arguments: path to source folder, path to replica folder, interval between synchronizations in seconds,  amount of synchronizations,  path to log file.")
    else:
        source_path, replica_path, interval, amount, log_file_path = args[1:]
        main(source_path, replica_path, interval, amount, log_file_path)
