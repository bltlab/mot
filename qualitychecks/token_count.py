#! /usr/bin/env python
"""
Script to observe token counts by language
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

MINIMUM_TOKEN_THRESHOLD = 10


def count_tokens(path: str) -> tuple[str, int]:
    """Retrieves language and number of tokens for each file"""
    print(f"Extracting data from {path}")
    with open(path) as file:
        data = json.load(file)
        language = data.get("site_language")
        tokens = data.get("tokens")
        n_tokens = 0
        if tokens:
            n_tokens = sum([len(sentence_tokens) for sentence in tokens for sentence_tokens in sentence])
        return language, n_tokens


class TokenCounter:
    def __init__(self) -> None:
        # Distribution of token counts by language
        self.token_distribution: Counter[str] = Counter()

    def count(self, language: str, n_tokens: int) -> None:
        """Counts total tokens by language"""
        print(f"Counting {language}")
        self.token_distribution[language] += n_tokens

    def histogram(self) -> None:
        """Creates histogram from sentence counts distribution by language"""
        df = pd.DataFrame.from_records(
            self.token_distribution.most_common(), columns=["Language", "Token Count"]
        )
        df.to_csv('token_count.csv')
        df.drop(df[df['Language'] == 'bam'].index, inplace=True)
        print(df[df["Token Count"] < MINIMUM_TOKEN_THRESHOLD])
        df.drop(df[df["Token Count"] < MINIMUM_TOKEN_THRESHOLD].index, inplace=True)
        print(df)
        sns.set_style("whitegrid")
        sns.barplot(
            data=df.reset_index(), x="Language", y="Token Count", color="#69b3a2"
        )
        plt.xticks(
            rotation=90,
            horizontalalignment="center"
        )
        plt.tight_layout()
        plt.autoscale()
        plt.savefig('token_count.png', dpi=200)
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

    counter = TokenCounter()

    if args.n_workers == 1:
        for path in find_docpaths(args.inputdir):
            language, n_tokens = count_tokens(path)
            counter.count(language, n_tokens)
    else:
        with Pool(args.n_workers) as pool:
            for language, n_tokens in pool.imap_unordered(count_tokens, find_docpaths(args.inputdir), chunksize=100):
                counter.count(language, n_tokens)

    counter.histogram()


if __name__ == "__main__":
    run()
