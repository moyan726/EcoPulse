import os
import glob
import time
from concurrent.futures import ProcessPoolExecutor

def count_lines_in_chunk(file_path, chunk_size=1024*1024*8):
    """
    Counts lines in a file by reading binary chunks.
    This is significantly faster than iterating line by line for massive files.
    """
    count = 0
    try:
        with open(file_path, 'rb') as f:
            while True:
                buffer = f.read(chunk_size)
                if not buffer:
                    break
                count += buffer.count(b'\n')
        return file_path, count
    except Exception as e:
        return file_path, -1

def main():
    target_dir = os.path.join("data", "row")
    # Match both .csv and .txt just in case, though we saw .csv
    files = glob.glob(os.path.join(target_dir, "*.csv"))
    
    if not files:
        print(f"No .csv files found in {target_dir}")
        return

    print(f"Found {len(files)} files. Starting statistical analysis...")
    print("-" * 60)
    print(f"{'File Name':<40} | {'Row Count':>15}")
    print("-" * 60)

    start_time = time.time()
    total_rows = 0
    
    # Use all available CPU cores for parallel processing
    # This is crucial for 200M+ rows distributed across multiple files
    with ProcessPoolExecutor() as executor:
        results = executor.map(count_lines_in_chunk, files)
        
        for file_path, count in results:
            file_name = os.path.basename(file_path)
            if count == -1:
                print(f"{file_name:<40} | {'ERROR':>15}")
            else:
                print(f"{file_name:<40} | {count:>15,}")
                total_rows += count

    end_time = time.time()
    duration = end_time - start_time

    print("-" * 60)
    print(f"{'TOTAL':<40} | {total_rows:>15,}")
    print("-" * 60)
    print(f"Time elapsed: {duration:.2f} seconds")
    print(f"Note: Counts include header rows (if present).")

if __name__ == "__main__":
    main()
