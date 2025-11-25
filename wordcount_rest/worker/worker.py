# worker_rest/worker.py
from fastapi import FastAPI
from pydantic import BaseModel
from collections import Counter
import re

app = FastAPI()

class WordCountRequest(BaseModel):
    text_chunk: str

class WordCountReply(BaseModel):
    counts: dict[str, int]

@app.post("/count_words", response_model=WordCountReply)
def count_words(req: WordCountRequest):
    tokens = re.findall(r"\w+", req.text_chunk.lower())
    counter = Counter(tokens)
    return WordCountReply(counts=dict(counter))
