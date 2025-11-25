import time
import grpc
import os
import sys
from concurrent import futures
from collections import Counter

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(ROOT_DIR)

import wordcount_pb2
import wordcount_pb2_grpc


WORKER_ADDRESSES = [
    "10.212.200.150:6001",
    "10.212.200.150:6002",
]


def split_text_into_chunks(text: str, n_chunks: int):
    lines = text.splitlines()
    chunk_size = max(1, len(lines) // n_chunks)
    chunks = []

    for i in range(0, len(lines), chunk_size):
        chunk = "\n".join(lines[i:i + chunk_size])
        chunks.append(chunk)

    if len(chunks) > n_chunks:
        chunks = chunks[:n_chunks]
    elif len(chunks) < n_chunks:
        chunks.extend([""] * (n_chunks - len(chunks)))

    return chunks


def count_words_distributed(text: str, worker_addresses):
    n_workers = len(worker_addresses)
    chunks = split_text_into_chunks(text, n_workers)

    channels = [grpc.insecure_channel(addr) for addr in worker_addresses]
    stubs = [wordcount_pb2_grpc.WordCountStub(ch) for ch in channels]

    start_time = time.perf_counter()  # start timing

    with futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
        future_to_worker = {}

        for stub, chunk, addr in zip(stubs, chunks, worker_addresses):
            request = wordcount_pb2.WordCountRequest(text_chunk=chunk)
            future = executor.submit(stub.CountWords, request)
            future_to_worker[future] = addr

        global_counter = Counter()

        for future in futures.as_completed(future_to_worker):
            addr = future_to_worker[future]
            try:
                response = future.result()
                global_counter.update(response.counts)
            except Exception as e:
                print(f"Error from worker {addr}: {e}")

    end_time = time.perf_counter()  # end timing
    elapsed = end_time - start_time

    return global_counter, elapsed

def main():
    text_path = os.path.join(ROOT_DIR, "shakespeare.txt")
    with open(text_path, "r", encoding="utf-8") as f:
        text = f.read()

    data_size_bytes = len(text.encode("utf-8"))

    print("Starting distributed word count...")
    total_counts, elapsed = count_words_distributed(text, WORKER_ADDRESSES)

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

