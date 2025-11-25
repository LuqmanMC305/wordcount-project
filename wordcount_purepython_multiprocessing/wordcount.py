from multiprocessing import Pool
import os
import re
import time
from collections import Counter


def split_text_into_chunks(text: str, n_chunks: int):
    lines = text.splitlines()
    chunk_size = max(1, len(lines) // n_chunks)
    chunks = []

    for i in range(0, len(lines), chunk_size):
        chunks.append("\n".join(lines[i:i + chunk_size]))

    while len(chunks) < n_chunks:
        chunks.append("")

    return chunks[:n_chunks]


def count_words_in_chunk(chunk):
    tokens = re.findall(r"\w+", chunk.lower())
    return Counter(tokens)


def count_words_multiprocessing(text, n_workers):
    chunks = split_text_into_chunks(text, n_workers)
    start = time.perf_counter()

    with Pool(n_workers) as pool:
        partial_counts = pool.map(count_words_in_chunk, chunks)

    global_counts = Counter()
    for c in partial_counts:
        global_counts.update(c)

    end = time.perf_counter()
    elapsed = end - start

    return global_counts, elapsed


def main():
    text_path = "shakespeare.txt"
    with open(text_path, "r", encoding="utf-8") as f:
        text = f.read()

    data_size_bytes = len(text.encode("utf-8"))

    print("Running multiprocessing baseline...")
    counts, elapsed = count_words_multiprocessing(text, n_workers=4)

    print("\nTop 20 words:")
    for word, cnt in counts.most_common(20):
        print(f"{word}: {cnt}")

    print(f"\nElapsed time: {elapsed:.4f} seconds")
    throughput_mb = (data_size_bytes / elapsed) / (1024 * 1024)
    print(f"Throughput: {throughput_mb:.4f} MB/s")


if __name__ == "__main__":
    main()
