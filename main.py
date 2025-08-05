import os
import json
import shutil
import sys
import time
from datetime import datetime

def create_dir(path):
    if not os.path.exists(path):
        os.mkdirs(path, exist_ok=True)


def copy_and_overwrite(source, dest):
    if os.path.exists(dest):
        shutil.rmtree(dest)
    shutil.copytree(source, dest)


def write_log_entry(log_file_path, action, details):
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "action": action,
        "details": details 
    }

    log_dir = os.path.dirname(log_file_path)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    try:
        if os.path.exists(log_file_path):
            with open(log_file_path, "r") as f:
                logs = json.load(f)
        else:
            logs = []
    except Exception as e:
        print(f"Warning: Could not write to log file: {e}")
    logs.append(log_entry)

    try:
        with open(log_file_path, "w") as f:
            json.dump(logs, f, indent=2)
    except Exception as e:
        print(f"Warning: Could not write to a log file: {e}")


def synchronize(source_path, replica_path, log_file_path):
    try:
        if not os.path.exists(source_path):
            raise Exception(f"Source path does not exist: {source_path}")
        
        print(f"Synchronizing {source_path} -> {replica_path}")
        copy_and_overwrite(source_path, replica_path)

        file_count = sum(len(files) for _, _, files in os.walk(replica_path))
        dir_count = sum(len(dirs) for _, dirs, _ in os.walk(replica_path))

        details = {
            "source": source_path,
            "replica": replica_path,
            "files copied": file_count,
            "directories created": dir_count
        }
        write_log_entry(log_file_path, "synchronization_complete", details)

        print(f"Copied {file_count} files and {dir_count} directories")

    except Exception as e:
        error_details = {
            "source": source_path,
            "replica": replica_path,
            "error": str(e)
        }
        write_log_entry(log_file_path, "synchronization_failed", error_details)
        print(f"Synchronization failed: {e}")
        raise


def main(source_path, replica_path, interval, amount, log_file_path):

    interval = int(interval)
    amount = int(amount)
    iteration: int = 0

    for iteration in range(amount):

        print(f"\nIteration: {iteration + 1}/{amount}")

        synchronize(source_path, replica_path, log_file_path)

        if iteration < amount - 1:
            print(f"Waiting {interval} seconds until next sync...")
            time.sleep(interval)
        print("\n" + "=" * 50)
        print("Synchronization process completed successfully!")


if __name__ == "__main__":
    args = sys.argv

    if len(args) != 6:
        raise Exception("You must pass 5 arguments: path to source folder, path to replica folder, interval between synchronizations,  amount of synchronizations,  path to log file.")

    source_path, replica_path, interval, amount, log_file_path = args[1:]

    main(source_path, replica_path, interval, amount, log_file_path)