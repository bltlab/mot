#! /usr/bin/env python
"""
Script to observe document counts by language and content type
"""

import os
import json
from argparse import ArgumentParser
from collections import Counter, defaultdict
from multiprocessing import Pool
from typing import Generator, Tuple
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def count_docs(path: str, key: str) -> Tuple[str, str]:
    """Retrieves language and content type for each file"""
    print(f"Extracting data from {path}")
    try:
        with open(path, 'r', encoding="utf8") as file:
            data = json.load(file)
            if key == "language":
                language = data.get("site_language")
            if key == "domain":
                language = path.split("/")[-3]
            content_type = data.get("content_type").capitalize()
            return language, content_type
    except json.decoder.JSONDecodeError:
        print(f"Json decode error for: {path}")


class DocumentCounter:
    def __init__(self) -> None:
        # Distribution of document counts by language and content type
        self.doc_content_distribution: defaultdict[str, Counter[str]] = defaultdict(
            Counter
        )

    def count(self, language: str, content_type: str) -> None:
        """Counts each document by language or domain"""
        print(f"Counting {language}")
        self.doc_content_distribution[language][content_type] += 1

    def histogram(self) -> None:
        """Creates histogram from document counts distribution by language or domain"""
        df = pd.DataFrame.from_dict(self.doc_content_distribution, orient="index")
        df.index.name = "Lang."
        df.sort_values(by=["Lang."], inplace=True)
        df.sort_index(axis=1, inplace=True)
        df['Total'] = df.sum(axis=1)
        print(df.convert_dtypes().reset_index())
        with open("content_type_count.tex", "w") as file:
            file.write(
                df.fillna(0).reset_index().convert_dtypes().applymap(lambda x: f'{x:,d}' if isinstance(x, int) else x).to_latex(index=False)
            )
        df.fillna(0).convert_dtypes().to_csv("content_type_count.csv")

        df.drop('Total', axis=1, inplace=True)
        df = pd.melt(
            df.reset_index(),
            id_vars="Lang.",
            var_name="Content Type",
            value_name="Count",
        )
        df["Log Count"] = np.log10(df["Count"])
        sns.set_style("whitegrid")
        sns.catplot(
            x="Lang.",
            y="Log Count",
            hue="Content Type",
            data=df,
            kind="bar",
            height=2.5,
            aspect=6,
            legend=False,
        )

        plt.xticks(
            rotation=90,
            horizontalalignment="center"
        )
        plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)
        plt.ylabel("Log Count")
        plt.tight_layout()
        plt.autoscale()
        plt.savefig("content_type_count.png", dpi=200)
        plt.show()


def find_docpaths(inputdir: str) -> Generator[str, None, None]:
    for root, dirs, files in os.walk(inputdir):
        for file in files:
            if file.endswith(".json"):
                yield os.path.join(root, file)


def run() -> None:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputdir",
        help="Input directory containing json in directories by iso_site/type",
    )
    parser.add_argument(
        "--key",
        type=str,
        default="language",
        help="Choose to display results by 'language' or 'domain'",
    )
    parser.add_argument(
        "--n-workers", type=int, default=1, help="Processes for multiprocessing"
    )
    args = parser.parse_args()

    counter = DocumentCounter()

    if args.n_workers == 1:
        for path in find_docpaths(args.inputdir):
            language, content_type = count_docs(path, args.key)
            counter.count(language, content_type)
    else:
        with Pool(args.n_workers) as pool:
            for language, content_type in pool.starmap_async(
                count_docs,
                [(path, args.key) for path in find_docpaths(args.inputdir)],
                chunksize=100,
            ).get():
                counter.count(language, content_type)

    counter.histogram()


if __name__ == "__main__":
    run()
