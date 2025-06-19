import os
import subprocess
import argparse
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
    # Get list of parquet files in the input folder
    parquet_files = [f for f in Path(input_folder).glob('*.parquet') if f.is_file()]
    
    if not parquet_files:
        print(f"No parquet files found in {input_folder}")
        return
    
    # Read schema from first file
    first_file = pq.read_table(parquet_files[0], memory_map=True)
    schema = first_file.schema
    
    # Create a Parquet writer with the same schema
    writer = pq.ParquetWriter(output_file, schema)
    
    try:
        # Process files one by one to minimize memory usage
        for file_path in parquet_files:
            # Use memory_map to avoid loading entire file into memory
            table = pq.read_table(file_path, memory_map=True)
            writer.write_table(table)
            
            if delete_original:
                os.remove(file_path)
                print(f"Processed and deleted {file_path}")
            else:
                print(f"Processed {file_path}")
                
    finally:
        # Ensure the writer is closed properly
        writer.close()
    
    print(f"Successfully concatenated {len(parquet_files)} files into {output_file}")

def get_current_output_folder(base_name=None):
    """Get the current output folder path based on date and optional website name"""
    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day
    
    if base_name:
        # If you want to include the website name
        folder = Path(f"data/year={year}/month={month}/day={day}/website={base_name}")
    else:
        # Just the date part
        folder = Path(f"data/year={year}/month={month}/day={day}")
    
    return str(folder.resolve())


def run_spider_in_batches(website, batch_size=1, max_batches=None, wait_time=50):
    batch_count = 0
    while True:
        if max_batches and batch_count >= max_batches:
            break
            
        print(f"Starting batch {batch_count + 1} for {website}")
        cmd = [
            'scrapy', 'crawl', website,
            '-s', f'CLOSESPIDER_ITEMCOUNT={batch_size}',
            '-s', f'JOBDIR=crawls/{website}'
        ]
        subprocess.run(cmd)
        
        batch_count += 1
        print(f"Batch {batch_count} for {website} completed. Waiting {wait_time} seconds...")
        sleep(wait_time)

    # At the end, concatenate all the parquet files
    output_folder = get_current_output_folder(website)
    concatenate_parquet_files(
        input_folder=output_folder,
        output_file=f"{output_folder}/combined.parquet",
        delete_original=True
    )

if __name__ == '__main__':
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Run Scrapy spider in batches')
    parser.add_argument('website', type=str, help='Name of the spider to run')
    parser.add_argument('--batch_size', type=int, default=50, help='Items per batch')
    parser.add_argument('--max_batches', type=int, default=None, help='Maximum number of batches')
    parser.add_argument('--wait_time', type=int, default=10, help='Seconds between batches')
    
    args = parser.parse_args()
    
    # Create necessary directories
    os.makedirs('crawls', exist_ok=True)
    
    # Run the spider
    run_spider_in_batches(
        website=args.website,
        batch_size=args.batch_size,
        max_batches=args.max_batches,
        wait_time=args.wait_time
    )