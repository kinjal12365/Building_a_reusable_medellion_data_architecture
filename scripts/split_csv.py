"""
split_csv.py
------------
Splits a CSV file into equal chunks for pipeline testing.
Usage: python scripts/split_csv.py --input <path> --output <folder> --chunks <n>
"""

import argparse
import pandas as pd
import os
import math

def split_csv(input_path, output_folder, num_chunks):
    # Read the full CSV
    df = pd.read_csv(input_path)
    total_rows = len(df)
    chunk_size = math.ceil(total_rows / num_chunks)

    print(f"Total rows: {total_rows}")
    print(f"Chunks: {num_chunks}")
    print(f"Rows per chunk: {chunk_size}")

    # Create output folder if not exists
    os.makedirs(output_folder, exist_ok=True)

    # Split and save
    for i in range(num_chunks):
        start = i * chunk_size
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end]

        output_path = os.path.join(output_folder, f"insurance_chunk_{i+1}.csv")
        chunk.to_csv(output_path, index=False)
        print(f"Saved chunk {i+1}: rows {start+1} to {end} → {output_path}")

    print(f"\nDone! {num_chunks} chunks saved to: {output_folder}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  required=True, help="Path to input CSV")
    parser.add_argument("--output", required=True, help="Output folder for chunks")
    parser.add_argument("--chunks", type=int, default=5, help="Number of chunks")
    args = parser.parse_args()

    split_csv(args.input, args.output, args.chunks)