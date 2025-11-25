import os
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed


def split_text_into_chunks(text: str, n_chunks: int):
    lines = text.splitlines()
    chunk_size = max(1, len(lines) // n_chunks)
    chunks = []

    for i in range(0, len(lines), chunk_size):
        chunks.append("\n".join(lines[i:i + chunk_size]))

    while len(chunks) < n_chunks:
        chunks.append("")

    return chunks[:n_chunks]


def count_words_in_chunk(text_chunk: str) -> dict[str, int]:
    tokens = re.findall(r"\w+", text_chunk.lower())
    counter = Counter(tokens)
    return counter


def count_words_single_machine(text: str, n_workers: int):
    chunks = split_text_into_chunks(text, n_workers)
    global_counter = Counter()

    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = [executor.submit(count_words_in_chunk, chunk)
                   for chunk in chunks]

        for fut in as_completed(futures):
            result = fut.result()
            global_counter.update(result)

    end = time.perf_counter()
    elapsed = end - start

    return global_counter, elapsed


def main():
    root_dir = os.path.dirname(os.path.abspath(__file__))
    text_path = os.path.join(root_dir, "shakespeare.txt")

    with open(text_path, "r", encoding="utf-8") as f:
        text = f.read()

    data_size_bytes = len(text.encode("utf-8"))
    n_workers = 4  # You can change this

    print("Running single-machine, no-network word count...")

    counts, elapsed = count_words_single_machine(text, n_workers)

    print("\nTop 20 words:")
    for word, cnt in counts.most_common(20):
        print(f"{word}: {cnt}")

    print(f"\nTotal unique words: {len(counts)}")
    print(f"Elapsed time: {elapsed:.4f} seconds")

    throughput_mb = (data_size_bytes / elapsed) / (1024 * 1024)
    print(f"Data size: {data_size_bytes} bytes")
    print(f"Throughput: {throughput_mb:.4f} MB/s")


if __name__ == "__main__":
    main()
