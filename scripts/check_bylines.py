#! /usr/bin/env python
"""
Check the bylines for a language in mongoDB.
"""
from argparse import ArgumentParser
from collections import Counter

from pymongo import MongoClient
from pymongo.collection import Collection


def check_names():
    parser = ArgumentParser()
    parser.add_argument("outpath")
    parser.add_argument("--language")
    args = parser.parse_args()

    client = MongoClient(port=27200)
    voa_corpus = client.voa_corpus
    sitemap_collection: Collection = voa_corpus.sitemaps
    byline_counts = Counter()
    for doc in sitemap_collection.find(
        {
            "iso": args.language,
            "success": True,
        }
    ):
        byline_counts[doc["utag_data"].get("byline", "")] += 1
    with open(args.outpath, "w", encoding="utf-8") as outfile:
        for item, count in byline_counts.most_common():
            print(f"{item}\t{count}", file=outfile)


if __name__ == "__main__":
    check_names()
