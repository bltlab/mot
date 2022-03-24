#! /usr/bin/env python
"""
Script to observe sentence counts by language
"""

import os
import json
from argparse import ArgumentParser
from collections import Counter
from multiprocessing import Pool
from typing import Generator
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

MINIMUM_SENTENCE_THRESHOLD = 10


def count_sentences(path: str) -> tuple[str, int]:
    """Retrieves language and number of sentences for each file"""
    print(f"Extracting data from {path}")
    with open(path) as file:
        data = json.load(file)
        language = data.get("site_language")
        sentences = data.get("sentences")
        n_sentences = 0
        if sentences:
            n_sentences = len(sentences)
        return language, n_sentences


class SentenceCounter:
    def __init__(self) -> None:
        # Distribution of sentence counts by language
        self.sentence_distribution: Counter[str] = Counter()

    def count(self, language: str, n_sentences: int) -> None:
        """Counts total sentences by language"""
        print(f"Counting {language}")
        self.sentence_distribution[language] += n_sentences

    def histogram(self) -> None:
        """Creates histogram from sentence counts distribution by language"""
        df = pd.DataFrame.from_records(
            self.sentence_distribution.most_common(), columns=["Language", "Sentence Count"]
        )
        df.to_csv('sentence_count.csv')
        df.drop(df[df['Language'] == 'bam'].index, inplace=True)
        print(df[df["Sentence Count"] < MINIMUM_SENTENCE_THRESHOLD])
        df.drop(df[df["Sentence Count"] < MINIMUM_SENTENCE_THRESHOLD].index, inplace=True)
        print(df)
        sns.set_style("whitegrid")
        sns.barplot(
            data=df.reset_index(), x="Language", y="Sentence Count", color="#69b3a2"
        )
        plt.xticks(
            rotation=90,
            horizontalalignment="center"
        )
        plt.tight_layout()
        plt.autoscale()
        plt.savefig('sentence_count.png', dpi=200)
        plt.show()


def find_docpaths(inputdir: str) -> Generator[str, None, None]:
    for root, dirs, files in os.walk(inputdir):
        if root.split("/")[-1] == "article":
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

    counter = SentenceCounter()

    if args.n_workers == 1:
        for path in find_docpaths(args.inputdir):
            language, n_sentences = count_sentences(path)
            counter.count(language, n_sentences)
    else:
        with Pool(args.n_workers) as pool:
            for language, n_sentences in pool.imap_unordered(count_sentences, find_docpaths(args.inputdir), chunksize=100):
                counter.count(language, n_sentences)

    counter.histogram()


if __name__ == "__main__":
    run()
