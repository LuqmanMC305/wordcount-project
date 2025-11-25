import argparse
from concurrent import futures
import grpc
import re
from collections import Counter

import sys
import os

# Ensure imports work if run from worker/ directory
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(ROOT_DIR)

import wordcount_pb2
import wordcount_pb2_grpc


class WordCountServicer(wordcount_pb2_grpc.WordCountServicer):
    def CountWords(self, request, context):
        text = request.text_chunk

        # Simple tokenization: words made of letters/numbers/underscore
        tokens = re.findall(r"\w+", text.lower())
        counter = Counter(tokens)

        counts = dict(counter)
        return wordcount_pb2.WordCountReply(counts=counts)


def serve(port: int):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    wordcount_pb2_grpc.add_WordCountServicer_to_server(WordCountServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    print(f"Worker listening on port {port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6001, help="Port to listen on")
    args = parser.parse_args()

    serve(args.port)
