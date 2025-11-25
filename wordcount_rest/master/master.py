# master/master.py
import time
import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

'''
FOR WITHOUT DOCKER
WORKER_ADDRESSES = [
    "localhost:6001",
    "localhost:6002",
]
'''

# Default for non-Docker runs
default_workers = "http://localhost:6001,http://localhost:6002"
workers_env = os.getenv("WORKER_ADDRESSES", default_workers)
WORKER_ADDRESSES = workers_env.split(",")


def split_text_into_chunks(text: str, n_chunks: int):
    lines = text.splitlines()
    if n_chunks <= 0:
        return [text]

    chunk_size = max(1, len(lines) // n_chunks)
    chunks = []

    for i in range(0, len(lines), chunk_size):
        chunks.append("\n".join(lines[i:i + chunk_size]))

    while len(chunks) < n_chunks:
        chunks.append("")

    return chunks[:n_chunks]


def call_worker(worker_base_url: str, text_chunk: str) -> dict[str, int]:
    url = worker_base_url + "/count_words"
    resp = requests.post(url, json={"text_chunk": text_chunk})
    resp.raise_for_status()
    data = resp.json()
    return data["counts"]


def count_words_distributed_rest(text: str, worker_addresses):
    chunks = split_text_into_chunks(text, len(worker_addresses))
    global_counter = Counter()

    with ThreadPoolExecutor(max_workers=len(worker_addresses)) as executor:
        futures = [
            executor.submit(call_worker, addr, chunk)
            for addr, chunk in zip(worker_addresses, chunks)
        ]

        for fut in as_completed(futures):
            counts = fut.result()
            global_counter.update(counts)

    return global_counter


def main():
    # read input text
    root_dir = os.path.dirname(os.path.dirname(__file__))
    text_path = os.path.join(root_dir, "shakespeare.txt")
    with open(text_path, "r", encoding="utf-8") as f:
        text = f.read()

    data_size_bytes = len(text.encode("utf-8"))

    print("Starting REST-based distributed word count...")

    # wait a bit for workers to start, prevent race conditions
    time.sleep(5)

    start_time = time.perf_counter()
    total_counts = count_words_distributed_rest(text, WORKER_ADDRESSES)
    end_time = time.perf_counter()
    elapsed = end_time - start_time

    print("\nTop 20 words:")
    for word, cnt in total_counts.most_common(20):
        print(f"{word}: {cnt}")

    print(f"\nTotal unique words: {len(total_counts)}")
    print(f"Elapsed time: {elapsed:.4f} seconds")

    if elapsed > 0:
        throughput_bytes = data_size_bytes / elapsed
        throughput_mb = throughput_bytes / (1024 * 1024)
        print(f"Data size: {data_size_bytes} bytes")
        print(f"Throughput: {throughput_mb:.4f} MB/s")


if __name__ == "__main__":
    main()


