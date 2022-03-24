#! /usr/bin/env python
"""
Script to observe mean number of characters by language
"""

import os
import json
from argparse import ArgumentParser
from collections import Counter, defaultdict
from multiprocessing import Pool
from typing import Generator
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

MINIMUM_MEAN_CHARACTER_THRESHOLD = 10


def average_chars(path: str) -> tuple[str, str]:
    """Retrieves language and number of characters for each file"""
    print(f"Extracting data from {path}")
    with open(path) as file:
        data = json.load(file)
        language = data.get("site_language")
        n_chars = data.get("n_chars")
        return language, n_chars


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
        plt.xticks(
            rotation=90,
            horizontalalignment="center"
        )
        plt.tight_layout()
        plt.autoscale()
        plt.savefig("character_average.png", dpi=200)
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

    counter = CharacterAverage()

    if args.n_workers == 1:
        for path in find_docpaths(args.inputdir):
            language, n_chars = average_chars(path)
            counter.count(language, n_chars)
    else:
        with Pool(args.n_workers) as pool:
            for language, n_chars in pool.imap_unordered(
                average_chars, find_docpaths(args.inputdir), chunksize=100
            ):
                counter.count(language, n_chars)

    counter.histogram()


if __name__ == "__main__":
    run()
