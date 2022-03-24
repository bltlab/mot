#! /usr/bin/env python

"""
Script to count number of articles in each section of corpus.
"""
import os
from argparse import ArgumentParser
from collections import Counter


def count_files(directory: str) -> int:
    count = 0
    for root, dirs, files in os.walk(directory):
        for f in files:
            # Don't count zips of dir or empty outputs
            if f.endswith(".tgz") or f.startswith("empty_output"):
                continue
            count += 1
    return count


def iso_from_section_name(section: str) -> str:
    pieces = section.split("_")
    if len(pieces) < 2:
        raise ValueError(f"Couldn't properly split {section}")
    return pieces[0]


def corpus_stats():
    parser = ArgumentParser()
    parser.add_argument("corpus_dir")
    parser.add_argument("--latex", action="store_true")
    args = parser.parse_args()

    counts = Counter()

    sections = os.listdir(args.corpus_dir)
    for section in sections:
        # Skip counting the tar of all languages
        if section.startswith("all"):
            continue
        counts[iso_from_section_name(section)] += count_files(
            os.path.join(args.corpus_dir, section)
        )

    if not args.latex:
        for iso in counts:
            print(iso, counts[iso])
    else:

        # print(
        #     f"""
        #     \\begin{{table}}[tb]
        #     \\centering
        #     \\begin{{tabular}}{{l*{{5}}r}}
        #     \\toprule
        #     Dataset & {'&'.join(sorted(counts))} \\\\
        #     \\midrule
        #     """
        # )
        #
        # print(f"Wikipedia & {' & '.join(['-' for _ in counts])} \\\\")
        # print(f"Wikipedia & {' & '.join([str(counts[iso]) for iso in counts])} \\\\")
        # print(
        #     """
        #     \\bottomrule
        #     \\end{tabular}
        #     \\caption{Comparison datasets by number of documents.}
        #     \\label{tab:mot-wikipedia}
        #     \\end{table}
        #     """
        # )

        print(
            """
            \\begin{table}[tb]
            \\centering
            \\begin{tabular}{l*{5}r}
            \\toprule
            Lang. & Wikipedia & MOT \\\\
            \\midrule
            """
        )
        for iso in sorted(counts):
            print(f"{iso} & - & {counts[iso]} \\\\")
        print(
            """
            \\bottomrule
            \\end{tabular}
            \\caption{Comparison datasets by number of documents.}
            \\label{tab:mot-wikipedia}
            \\end{table}
            """
        )


if __name__ == "__main__":
    corpus_stats()
