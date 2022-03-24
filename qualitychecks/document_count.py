#! /usr/bin/env python
"""
Script to observe article counts by language
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
import numpy as np

MINIMUM_ARTICLE_THRESHOLD = 10


def count_docs(path: str) -> str:
    """Retrieves language for each file"""
    print(f"Extracting data from {path}")
    with open(path) as file:
        data = json.load(file)
        language = data.get("site_language")
        return language


class DocumentCounter:
    def __init__(self) -> None:
        # Distribution of article counts by language
        self.doc_distribution: Counter[str] = Counter()

    def count(self, language: str) -> None:
        """Counts each article by language"""
        print(f"Counting {language}")
        self.doc_distribution[language] += 1

    def histogram(self) -> None:
        """Creates histogram from article counts distribution by language"""
        df = pd.DataFrame.from_records(
            self.doc_distribution.most_common(), columns=["Language", "Count"]
        )
        df.to_csv("document_count.csv")
        df.drop(df[df["Language"] == "bam"].index, inplace=True)
        print(df[df["Count"] < MINIMUM_ARTICLE_THRESHOLD])
        df.drop(df[df["Count"] < MINIMUM_ARTICLE_THRESHOLD].index, inplace=True)
        print(df)

        sns.set(rc={'figure.figsize': (12, 7)})
        sns.set(font_scale=1.5)
        sns.set_style("whitegrid")
        plt.figure()
        sns.barplot(data=df.reset_index(), x="Language", y="Count", color="#69b3a2")
        plt.xticks(
            rotation=90,
            horizontalalignment="center"
        )
        plt.tight_layout()
        plt.autoscale()
        plt.savefig("document_count.png", dpi=200)
        plt.show()

        plt.figure()
        df["Log Count"] = np.log10(df["Count"])
        sns.barplot(data=df.reset_index(), x="Language", y="Log Count", color="#69b3a2")
        plt.xticks(
            rotation=90,
            horizontalalignment="center"
        )
        plt.tight_layout()
        plt.autoscale()
        plt.savefig("document_log_count.png", dpi=200)
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

    counter = DocumentCounter()

    if args.n_workers == 1:
        for path in find_docpaths(args.inputdir):
            language = count_docs(path)
            counter.count(language)
    else:
        with Pool(args.n_workers) as pool:
            for language in pool.imap_unordered(
                count_docs, find_docpaths(args.inputdir), chunksize=100
            ):
                counter.count(language)

    counter.histogram()


if __name__ == "__main__":
    run()
