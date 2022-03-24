#! /usr/bin/env python
"""
Script to observe language ID counts by language
"""

import os
import json
from argparse import ArgumentParser
from collections import Counter, defaultdict
from multiprocessing import Pool
from typing import Generator
import pandas as pd


def extract_data(path: str, key: str) -> tuple[str, str]:
    """Retrieves language and cld3 detected languages for each file"""
    print(f"Extracting data from {path}")
    with open(path) as file:
        data = json.load(file)
        if key == "language":
            language = data.get("site_language")
        if key == "domain":
            language = path.split("/")[-3]
        cld3_languages = data.get('cld3_detected_languages')
        return language, cld3_languages


class LIDCounter:
    def __init__(self) -> None:
        # Distribution of LID counts by language
        self.lid_distribution: defaultdict[str, Counter[str]] = defaultdict(Counter)

    def count(self, language: str, cld3_languages: str) -> None:
        """Counts CLD3-identified languages by language"""
        print(f"Counting {language}")
        if cld3_languages:
            highest_probability = max(cld3_languages, key=lambda v: cld3_languages[v]['probability'])
            self.lid_distribution[language][highest_probability] += 1


    def total_counts(self) -> None:
        df = pd.DataFrame.from_dict(self.lid_distribution, orient="index")
        df.index.name = "Language"
        df.sort_values(by=["Language"], inplace=True)
        df.sort_index(axis=1, inplace=True)
        df = df.reset_index()
        df.fillna(0).convert_dtypes().to_csv('languageid_count.csv', index=False)


def find_docpaths(inputdir: str, filter: str) -> Generator[str, None, None]:
    for root, dirs, files in os.walk(inputdir):
        if filter == 'article':
            if root.split("/")[-1] == "article":
                for file in files:
                    if file.endswith(".json"):
                        yield os.path.join(root, file)
        else:
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
        help="Choose to display results by 'language' or 'domain'"
    )
    parser.add_argument("--filter", type=str, default='article', help="Choose to display results by 'article' or 'document'")
    parser.add_argument("--n-workers", type=int, default=1)
    args = parser.parse_args()

    counter = LIDCounter()

    if args.n_workers == 1:
        for path in find_docpaths(args.inputdir, args.filter):
            language, cld3_languages = extract_data(path, args.key)
            counter.count(language, cld3_languages)
    else:
        with Pool(args.n_workers) as pool:
            for language, cld3_languages in pool.starmap_async(
                extract_data,
                [(path, args.key) for path in find_docpaths(args.inputdir, args.filter)],
                chunksize=100,
            ).get():
                counter.count(language, cld3_languages)

    counter.total_counts()


if __name__ == "__main__":
    run()
