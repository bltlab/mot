#! /usr/bin/env python
"""
Script to observe mean number of characters by language
"""

import os
import json
from argparse import ArgumentParser
from collections import Counter, defaultdict
from multiprocessing import Pool
from typing import Generator, Tuple
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

MINIMUM_MEAN_CHARACTER_THRESHOLD = 10


def average_chars(article: str) -> int:
    """Retrieves language and number of characters for each file"""
    n_chars = len(article)
    return n_chars


class CharacterAverage:
    def __init__(self) -> None:
        # Distribution of articles by language
        self.doc_distribution: Counter[str] = Counter()
        # Distribution of mean characters by language
        self.mean_char_distribution: Counter[str] = Counter()

    def count(self, language: str, n_chars: str) -> None:
        """Calculates average number of characters by language"""
        print(f"Counting {language}")
        self.doc_distribution[language] += 1
        self.mean_char_distribution[language] = (
            (
                self.mean_char_distribution[language]
                * (self.doc_distribution[language] - 1)
            )
            + int(n_chars)
        ) / self.doc_distribution[language]

    def histogram(self) -> None:
        """Creates histogram from character average distribution by language"""
        df = pd.DataFrame.from_records(
            self.mean_char_distribution.most_common(),
            columns=["Language", "Mean Character"],
        )
        df.to_csv("character_average.csv")
        df.drop(df[df["Language"] == "bam"].index, inplace=True)
        print(df[df["Mean Character"] < MINIMUM_MEAN_CHARACTER_THRESHOLD])
        df.drop(
            df[df["Mean Character"] < MINIMUM_MEAN_CHARACTER_THRESHOLD].index,
            inplace=True,
        )
        print(df)
        sns.set_style("whitegrid")
        sns.barplot(
            data=df.reset_index(), x="Language", y="Mean Character", color="#69b3a2"
        )
        plt.xticks(rotation=90, horizontalalignment="center")
        plt.tight_layout()
        plt.autoscale()
        plt.savefig("character_average.png", dpi=200)
        plt.show()


def read_articles(inputfile: str) -> Generator[str, None, None]:
    with open(inputfile, "r", encoding="utf8") as infile:
        for line in infile:
            yield line


def run() -> None:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputdir",
        help="Dir with wikipedia corpora files by language",
    )
    parser.add_argument("--n-workers", type=int, default=1)
    args = parser.parse_args()

    counter = CharacterAverage()

    for root, dirs, files in os.walk(args.inputdir):
        for file in files:
            language = os.path.basename(file)[:3]
            if args.n_workers == 1:
                for article in read_articles(os.path.join(root, file)):
                    n_chars = average_chars(article)
                    counter.count(language, n_chars)
            else:
                with Pool(args.n_workers) as pool:
                    for n_chars in pool.imap_unordered(
                        average_chars,
                        read_articles(os.path.join(root, file)),
                        chunksize=100,
                    ):
                        counter.count(language, n_chars)

    counter.histogram()


if __name__ == "__main__":
    run()
