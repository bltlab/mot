#! /usr/bin/env python
"""
Checks the database and gets the min and max publication date
"""
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime

from extraction.utils import get_sitemap_collection


def check_dates():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("outpath")
    parser.add_argument("--port", type=int, default=27200)
    args = parser.parse_args()

    collection = get_sitemap_collection(port=args.port)
    max_date = None
    min_date = None
    date_dict = defaultdict(list)
    for doc in collection.find({}):
        date = doc.get("date_published")

        if date and type(date) == datetime:
            date_dict[date].append(doc.get("url"))
            if max_date is None:
                max_date = date
            if min_date is None:
                min_date = date
            if date > max_date:
                max_date = date
            if date < min_date:
                min_date = date

    print(f"Max date: {max_date}")
    print(f"Min date: {min_date}")
    with open(args.outpath, "w", encoding="utf8") as outfile:
        for d in sorted(date_dict):
            urls = "\t".join(date_dict[d])
            print(f"{d}\t{urls}", file=outfile)


if __name__ == "__main__":
    check_dates()
