import os
import subprocess
import argparse
import json
from time import sleep
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq


def concatenate_parquet_files(input_folder, output_file, delete_original=True):
    """
    Concatenates all parquet files in a folder into a single file efficiently.

    Args:
        input_folder (str): Path to folder containing parquet files
        output_file (str): Path for the output concatenated parquet file
        delete_original (bool): Whether to delete original files (default: True)

    Returns:
        None
    """
    # Get list of parquet files in the input folder (exclude combined.parquet)
    parquet_files = [f for f in Path(input_folder).glob('*.parquet')
                     if f.is_file() and f.name != 'combined.parquet']

    if not parquet_files:
        print(f"No parquet files to concatenate in {input_folder}")
        return

    print(f"Found {len(parquet_files)} parquet files to concatenate in {input_folder}")

    # Read schema from first file
    # Initialize with first file
    first_table = pq.read_table(parquet_files[0], memory_map=True)
    writer = pq.ParquetWriter(output_file, first_table.schema)

    try:
        for file_path in parquet_files:
            try:
                table = pq.read_table(file_path, memory_map=True)
                
                # Convert binary columns to string if needed
                for i in range(table.num_columns):
                    col_name = table.column_names[i]
                    if str(table.schema.types[i]) == 'binary':
                        table = table.set_column(
                            i,
                            col_name,
                            table.column(i).cast('string')
                        )
                
                writer.write_table(table)

                if delete_original:
                    os.remove(file_path)
                    print(f"Processed and deleted {file_path}")
                else:
                    print(f"Processed {file_path}")
                    
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
                continue

    finally:
        writer.close()

    print(f"Successfully concatenated files into {output_file}")


def get_current_output_folder(base_name=None):
    """Get the current output folder path based on date and optional website name."""
    today = datetime.now()
    year = today.year
    month = today.month
    day = today.day

    if base_name:
        today_folder = Path(f"data/year={year}/month={month}/day={day}/website={base_name}")
    else:
        today_folder = Path(f"data/year={year}/month={month}/day={day}")

    return str(today_folder.resolve())


def get_scraped_item_count(output_folder):
    """
    Count total items scraped by reading all parquet files in the output folder

    Args:
        output_folder (str): Path to the output folder containing parquet files

    Returns:
        int: Total number of items scraped
    """
    parquet_files = [f for f in Path(output_folder).glob('*.parquet') if f.is_file()]
    total_items = 0

    for file_path in parquet_files:
        try:
            table = pq.read_table(file_path, memory_map=True)
            total_items += len(table)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    return total_items


def check_scrapy_stats(spider_name):
    """
    Check Scrapy stats from the stats file to see if items were scraped in last run

    Args:
        spider_name (str): Name of the spider

    Returns:
        dict: Stats from the last run, or None if file doesn't exist
    """
    stats_file = Path(f"crawls/{spider_name}/spider.stats")
    if stats_file.exists():
        try:
            with open(stats_file, 'r') as f:
                stats = json.load(f)
            return stats
        except Exception as e:
            print(f"Error reading stats file: {e}")
    return None


def consolidate_all_directories(used_directories):
    """
    Consolidate parquet files in all directories that were used during scraping.

    Args:
        used_directories (set): Set of directory paths that were used during scraping
    """
    print(f"\n=== Starting consolidation of {len(used_directories)} directories ===")

    for directory in used_directories:
        directory_path = Path(directory)

        if not directory_path.exists():
            print(f"Directory {directory} no longer exists, skipping...")
            continue

        # Check if there are individual parquet files to consolidate
        individual_files = [f for f in directory_path.glob('*.parquet')
                            if f.is_file() and f.name != 'combined.parquet']

        if not individual_files:
            print(f"No individual parquet files to consolidate in {directory}")
            continue

        print(f"Consolidating {len(individual_files)} files in {directory}")

        # Create combined file path
        combined_file = directory_path / 'combined.parquet'

        # Consolidate the files
        concatenate_parquet_files(
            input_folder=str(directory_path),
            output_file=str(combined_file),
            delete_original=True
        )

        print(f"Consolidated files in {directory} -> {combined_file}")

    print("=== Consolidation completed ===\n")


def run_spider_in_batches(website, batch_size=1, max_batches=None, wait_time=50,
                          max_empty_batches=3, min_items_threshold=1):
    """
    Run spider in batches with automatic stopping when no new data is found

    Args:
        website (str): Name of the spider to run
        batch_size (int): Items per batch
        max_batches (int): Maximum number of batches
        wait_time (int): Seconds between batches
        max_empty_batches (int): Maximum consecutive batches with no new items before stopping
        min_items_threshold (int): Minimum items required in a batch to continue
    """
    batch_count = 0
    empty_batch_count = 0
    last_total_items = 0

    # Set to track all directories used during scraping
    used_directories = set()

    # Create necessary directories
    os.makedirs('crawls', exist_ok=True)

    while True:
        # Get current output folder (this might change if day changes)
        current_output_folder = get_current_output_folder(website)

        # Add current directory to our tracking set
        used_directories.add(current_output_folder)

        # Ensure current output folder exists
        os.makedirs(current_output_folder, exist_ok=True)

        if max_batches and batch_count >= max_batches:
            print(f"Reached maximum batches limit: {max_batches}")
            break

        print(f"Starting batch {batch_count + 1} for {website}")
        print(f"Current output directory: {current_output_folder}")

        # Count items before running the batch
        items_before = get_scraped_item_count(current_output_folder)

        # Run the spider
        cmd = [
            'scrapy', 'crawl', website,
            '-s', f'CLOSESPIDER_ITEMCOUNT={batch_size}',
            '-s', f'JOBDIR=crawls/{website}',
            '-s', 'LOG_LEVEL=DEBUG'  # Add logging to see what's happening
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Count items after running the batch
        items_after = get_scraped_item_count(current_output_folder)
        items_scraped_this_batch = items_after - items_before

        batch_count += 1
        print(f"Batch {batch_count} completed. Items scraped this batch: {items_scraped_this_batch}")
        print(f"Total items scraped in current directory: {items_after}")

        # Check if we scraped enough items in this batch
        if items_scraped_this_batch < min_items_threshold:
            empty_batch_count += 1
            print(f"Low/no items in batch {batch_count}. Empty batch count: {empty_batch_count}")

            # Check if spider has finished (no more URLs to process)
            if "Spider closed" in result.stdout or "closespider" in result.stdout.lower():
                print("Spider indicates it has finished crawling")
                break

            if empty_batch_count >= max_empty_batches:
                print(f"Reached maximum empty batches limit: {max_empty_batches}")
                print("Stopping spider - likely finished crawling all available data")
                break
        else:
            # Reset empty batch count if we got items
            empty_batch_count = 0

        # Additional check: if total items hasn't changed for several batches
        if items_after == last_total_items and batch_count > 1:
            print("No new items scraped in this batch, spider may have finished")
            break

        last_total_items = items_after

        # Wait before next batch (but not after the last batch)
        if empty_batch_count < max_empty_batches and (not max_batches or batch_count < max_batches):
            print(f"Waiting {wait_time} seconds before next batch...")
            sleep(wait_time)

    print(f"\nScraping completed after {batch_count} batches")
    print(f"Used directories during scraping: {len(used_directories)}")
    for directory in sorted(used_directories):
        item_count = get_scraped_item_count(directory)
        print(f"  - {directory}: {item_count} items")

    # Now consolidate all directories that were used
    consolidate_all_directories(used_directories)

    # Print final summary
    print("\n=== Final Summary ===")
    total_items_across_all_dirs = 0
    for directory in used_directories:
        # Count items in combined.parquet if it exists, otherwise count all parquet files
        combined_file = Path(directory) / 'combined.parquet'
        if combined_file.exists():
            try:
                table = pq.read_table(str(combined_file), memory_map=True)
                items_in_dir = len(table)
            except Exception as e:
                print(f"Error reading combined file in {directory}: {e}")
                items_in_dir = get_scraped_item_count(directory)
        else:
            items_in_dir = get_scraped_item_count(directory)

        total_items_across_all_dirs += items_in_dir
        print(f"Final items in {directory}: {items_in_dir}")

    print(f"Total items scraped across all directories: {total_items_across_all_dirs}")


if __name__ == '__main__':
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Run Scrapy spider in batches with auto-stop')
    parser.add_argument('website', type=str, help='Name of the spider to run')
    parser.add_argument('--batch_size', type=int, default=50, help='Items per batch')
    parser.add_argument('--max_batches', type=int, default=None, help='Maximum number of batches')
    parser.add_argument('--wait_time', type=int, default=10, help='Seconds between batches')
    parser.add_argument('--max_empty_batches', type=int, default=3,
                        help='Maximum consecutive batches with no new items before stopping')
    parser.add_argument('--min_items_threshold', type=int, default=1,
                        help='Minimum items required in a batch to continue')

    args = parser.parse_args()

    # Run the spider
    run_spider_in_batches(
        website=args.website,
        batch_size=args.batch_size,
        max_batches=args.max_batches,
        wait_time=args.wait_time,
        max_empty_batches=args.max_empty_batches,
        min_items_threshold=args.min_items_threshold
    )