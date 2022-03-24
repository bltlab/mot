#! /usr/bin/env python
"""
Script to observe sentence and token counts by language
"""

import os
import json
from argparse import ArgumentParser
from collections import Counter, defaultdict
from multiprocessing import Pool
from typing import Generator
import pandas as pd

MINIMUM_SENTENCE_THRESHOLD = 10
MINIMUM_TOKEN_THRESHOLD = 10


def count_sentences_tokens(path: str) -> tuple[str, int, int]:
    """Retrieves language, number of sentences, and number of tokens for each file"""
    print(f"Extracting data from {path}")
    with open(path) as file:
        data = json.load(file)
        language = data.get("site_language")
        sentences = data.get("sentences")
        n_sentences = 0
        if sentences:
            n_sentences = len(sentences)
        tokens = data.get("tokens")
        n_tokens = 0
        if tokens:
            n_tokens = sum([len(sentence_tokens) for sentence in tokens for sentence_tokens in sentence])
        return language, n_sentences, n_tokens


class SentenceTokenCounter:
    def __init__(self) -> None:
        # Distribution of document counts by language and content type
        self.doc_sentence_token_distribution: defaultdict[str, Counter[str]] = defaultdict(
            Counter
        )

    def count(self, language: str, n_sentences: int, n_tokens:int) -> None:
        """Counts total sentences and tokens by language"""
        print(f"Counting {language}")
        self.doc_sentence_token_distribution[language]['Document'] += 1
        self.doc_sentence_token_distribution[language]['Sentence'] += n_sentences
        self.doc_sentence_token_distribution[language]['Token'] += n_tokens

    def histogram(self) -> None:
        """Creates histogram from sentence and token count distributions by language"""
        df = pd.DataFrame.from_dict(self.doc_sentence_token_distribution, orient="index")
        df.index.name = "Language"
        df.sort_values(by=["Language"], inplace=True)
        df.sort_index(axis=1, inplace=True)
        df = df.reset_index()
        df.to_csv('sentence_token_count.csv')
        df.drop(df[df['Language'] == 'bam'].index, inplace=True)
        print(df[df["Sentence"] < MINIMUM_SENTENCE_THRESHOLD])
        df.drop(df[df["Sentence"] < MINIMUM_SENTENCE_THRESHOLD].index, inplace=True)
        print(df[df["Token"] < MINIMUM_TOKEN_THRESHOLD])
        df.drop(df[df["Token"] < MINIMUM_TOKEN_THRESHOLD].index, inplace=True)
        print(df)

        with open("sentence_token_count.tex", "w") as file:
            file.write(
                df.applymap(lambda x: f'{x:,d}' if isinstance(x, int) else x).to_latex(index=False)
            )

def find_docpaths(inputdir: str) -> Generator[str, None, None]:
    for root, dirs, files in os.walk(inputdir):
        # if root.split("/")[-1] == "article":
        for file in files:
            if file.endswith(".json"):
                yield os.path.join(root, file)


def run() -> None:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputdir",
        help="Input directory containing json in directories by iso_site/type",
    )
    parser.add_argument("--n-workers", type=int, default=1)
    args = parser.parse_args()

    counter = SentenceTokenCounter()

    if args.n_workers == 1:
        for path in find_docpaths(args.inputdir):
            language, n_sentences, n_tokens = count_sentences_tokens(path)
            counter.count(language, n_sentences, n_tokens)
    else:
        with Pool(args.n_workers) as pool:
            for language, n_sentences, n_tokens in pool.imap_unordered(count_sentences_tokens, find_docpaths(args.inputdir),
                                                             chunksize=100):
                counter.count(language, n_sentences, n_tokens)

    counter.histogram()


if __name__ == "__main__":
    run()
