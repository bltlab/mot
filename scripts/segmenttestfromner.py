#! /usr/bin/env python
"""
Script to take ner data (MasakhaNER) and make sentences one per line for segmentation
test data.
Attempts to re-attach punctuation to be more realistic eval.
This also will naively put tokens separated by spaces, so don't use for tokenization eval.
"""
import os
from argparse import ArgumentParser
from typing import List

PUNCTUATION = {
    ".",
    ",",
    "!",
    "?",
    ":",
    ";",
}


def write_sents(sents: List[str], outpath: str, newline=False):
    end = "\n" if newline else " "
    with open(outpath, 'w', encoding='utf8') as outfile:
        for sent in sents:
            print(sent, file=outfile, end=end)


def segmenttestfromner():
    parser = ArgumentParser()
    parser.add_argument("infile")
    parser.add_argument("outdir")
    args = parser.parse_args()

    sents = read_sents(args.infile)
    write_sents(sents, os.path.join(args.outdir, "sents.txt"), newline=True)
    write_sents(sents, os.path.join(args.outdir, "unsplit.txt"), newline=False)


def read_sents(inpath: str):
    sents = []
    tokens = []
    with open(inpath, 'r', encoding='utf8') as infile:
        for line in infile:
            fields = line.strip().split()
            token = fields[0] if fields else None
            if not fields:
                if tokens:
                    sents.append(" ".join(tokens))
                    tokens = []
            elif token in PUNCTUATION and tokens:
                last = tokens.pop()
                tokens.append("".join([last, token]))
            else:
                tokens.append(token)
    return sents


if __name__ == "__main__":
    segmenttestfromner()