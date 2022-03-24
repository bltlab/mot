#! /usr/bin/env python
"""
Script to observe article counts by date
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
import numpy as np

MINIMUM_ARTICLE_THRESHOLD = 10


def count_docs(path: str) -> tuple[str, str]:
    """Retrieves language and date for each file"""
    print(f"Extracting data from {path}")
    with open(path) as file:
        data = json.load(file)
        language = data.get("site_language")
        time_published = data.get("time_published")
        return language, time_published


class ArticleCounter:
    def __init__(self) -> None:
        # Distribution of article counts by language and date
        self.language_date_distribution: defaultdict[str, Counter[str]] = defaultdict(
            Counter
        )
        # Distribution of article counts by date (not in use)
        self.date_distribution: Counter[str] = Counter()
        # Distribution of article counts (with date_published field) by language (not in use)
        self.doc_distribution: Counter[str] = Counter()

    def count(self, language: str, time_published: str) -> None:
        """Counts each article by language and date"""
        print(f"Counting {language}")
        date = time_published[:4]
        self.language_date_distribution[language][date] += 1
        self.date_distribution[date] += 1
        self.doc_distribution[language] += 1

    def histogram(self) -> None:
        """Creates histogram from article count distribution by date"""
        for language in list(self.language_date_distribution):
            if language == "bam":
                print(f"Manually removed {language}")
                self.language_date_distribution.pop(language)
            elif (
                sum(self.language_date_distribution[language].values())
                < MINIMUM_ARTICLE_THRESHOLD
            ):
                print(f"Removed {language}")
                self.language_date_distribution.pop(language)

        date_distribution: Counter[str] = Counter()
        for language in self.language_date_distribution:
            date_distribution += self.language_date_distribution[language]

        df = pd.DataFrame.from_records(
            sorted(date_distribution.items()), columns=["Year", "Count"]
        )
        print(df)
        df.to_csv("date_count.csv")
        print(df[df["Count"] < MINIMUM_ARTICLE_THRESHOLD])
        df.drop(df[df["Count"] < MINIMUM_ARTICLE_THRESHOLD].index, inplace=True)

        sns.set(rc={'figure.figsize': (12, 8)})
        sns.set(font_scale=2)
        sns.set_style("whitegrid")
        plt.figure()
        sns.barplot(data=df.reset_index(), x="Year", y="Count", color="#69b3a2")
        plt.xticks(
            rotation=90,
            horizontalalignment="center"
        )
        plt.tight_layout()
        plt.autoscale()
        plt.savefig("date_count.png", dpi=200)
        plt.show()

        df["Log Count"] = np.log10(df["Count"])
        plt.figure()
        sns.barplot(data=df.reset_index(), x="Year", y="Log Count", color="#69b3a2")
        plt.xticks(
            rotation=90,
            horizontalalignment="center"
        )
        plt.tight_layout()
        plt.autoscale()
        plt.savefig("date_log_count.png", dpi=200)
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

    counter = ArticleCounter()

    if args.n_workers == 1:
        for path in find_docpaths(args.inputdir):
            if count_docs(path):
                language, time_published = count_docs(path)
                if time_published:
                    counter.count(language, time_published)
    else:
        with Pool(args.n_workers) as pool:
            for language, time_published in pool.imap_unordered(
                count_docs, find_docpaths(args.inputdir), chunksize=100
            ):
                if time_published:
                    counter.count(language, time_published)

    counter.histogram()


if __name__ == "__main__":
    run()
