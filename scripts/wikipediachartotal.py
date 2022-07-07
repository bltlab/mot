#! /usr/bin/env python
"""
Script to observe character counts by language
"""

import os
import json
from argparse import ArgumentParser
from collections import Counter
from multiprocessing import Pool
from typing import Generator, Tuple
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

MINIMUM_CHARACTER_THRESHOLD = 10


def count_chars(article: str) -> Tuple[str, str]:
    """Retrieves language and number of characters for each file"""
    n_chars = len(article)
    return n_chars


class CharacterCounter:
    def __init__(self) -> None:
        # Distribution of character counts by language
        self.char_distribution: Counter[str] = Counter()

    def count(self, language: str, n_chars: str) -> None:
        """Counts total characters by language"""
        print(f"Counting {language}")
        self.char_distribution[language] += int(n_chars)

    def histogram(self) -> None:
        """Creates histogram from character counts distribution by language"""
        df = pd.DataFrame.from_records(
            self.char_distribution.most_common(),
            columns=["Language", "Character Count"],
        )
        df.to_csv("character_count.csv")
        df.drop(df[df["Language"] == "bam"].index, inplace=True)
        print(df[df["Character Count"] < MINIMUM_CHARACTER_THRESHOLD])
        df.drop(
            df[df["Character Count"] < MINIMUM_CHARACTER_THRESHOLD].index, inplace=True
        )
        print(df)
        sns.set_style("whitegrid")
        sns.barplot(
            data=df.reset_index(), x="Language", y="Character Count", color="#69b3a2"
        )
        plt.xticks(rotation=90, horizontalalignment="center")
        plt.tight_layout()
        plt.autoscale()
        plt.savefig("character_count.png", dpi=200)
        plt.show()


def run() -> None:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputdir",
        help="Input directory containing json in directories by iso_site/type",
    )
    parser.add_argument("--n-workers", type=int, default=1)
    args = parser.parse_args()

    counter = CharacterCounter()
    for root, dirs, files in os.walk(args.inputdir):
        for file in files:
            language = os.path.basename(file)[:3]
            if args.n_workers == 1:
                for article in read_articles(os.path.join(root, file)):
                    language, n_chars = count_chars(article)
                    counter.count(language, n_chars)
            else:
                with Pool(args.n_workers) as pool:
                    for n_chars in pool.imap_unordered(
                        count_chars,
                        read_articles(os.path.join(root, file)),
                        chunksize=100,
                    ):
                        counter.count(language, n_chars)
    counter.histogram()


def read_articles(inputfile: str) -> Generator[str, None, None]:
    with open(inputfile, "r", encoding="utf8") as infile:
        for line in infile:
            yield line


if __name__ == "__main__":
    run()
