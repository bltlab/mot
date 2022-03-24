"""
Create a single TSV file with results of language identification for all languages using files from languageid.py
"""

import os
import csv
from argparse import ArgumentParser
from typing import Generator


def tsv(path: str, outdir: str) -> None:
    with open(path, newline="") as input_tsv_file:
        reader = csv.DictReader(input_tsv_file)

        output_tsv_name = os.path.join(outdir, "all_languages") + ".tsv"
        with open(output_tsv_name, "a", newline="") as output_tsv_file:
            fieldnames = [
                "domain",
                "file_name",
                "url",
                "is_supported",
                "paragraph#",
                "num_chars",
                "language1",
                "probability1",
                "unexpected1",
                "language2",
                "probability2",
                "unexpected2",
            ]
            writer = csv.DictWriter(
                output_tsv_file, fieldnames=fieldnames, restval="", extrasaction="raise"
            )

            if is_empty(output_tsv_name):
                writer.writeheader()

            # Assumes path is <directory>/<iso_code>.tsv
            iso_domain = path.split("/")[1].rstrip(".tsv")
            for row in reader:
                row["domain"] = iso_domain
                writer.writerow(row)

        print(f"Writing {path} to {outdir}")


def is_empty(file_path):
    return os.path.getsize(file_path) == 0


def find_docpaths(inputdir: str) -> Generator[str, None, None]:
    for root, dirs, files in os.walk(inputdir):
        for file in files:
            if file.endswith(".tsv"):
                yield os.path.join(root, file)


def run():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "dir", help="Input directory containing tsv files and output directory"
    )
    parser.add_argument("--n-workers", type=int, default=1)
    args = parser.parse_args()

    for path in find_docpaths(args.dir):
        tsv(path, args.dir)


if __name__ == "__main__":
    run()
