#! /usr/bin/env python
"""
Script to check tokenization
"""

import os
import json
from argparse import ArgumentParser
from collections import defaultdict, Counter
from multiprocessing import Pool
from typing import Generator


class TokenCounter:
    def __init__(self) -> None:
        # Distribution of sentence lengths by number of tokens by language/domain
        self.token_distribution: defaultdict[str, Counter[str]] = defaultdict(Counter)
        # Unique tokens by language/domain
        self.unique_tokens: defaultdict[str, set] = defaultdict()

    def count_tokens(self, path: str) -> None:
        """Counts and stores tokens in each json file"""
        with open(path) as file:
            iso_domain = path.split("/")[1]
            data = json.load(file)
            tokens = data.get('tokens')
            for sentence_tokens in tokens:
                self.token_distribution[iso_domain][len(sentence_tokens)] += 1
                for token in sentence_tokens:
                    if iso_domain not in self.unique_tokens:
                        self.unique_tokens[iso_domain] = set()
                    self.unique_tokens[iso_domain].add(token)
        print(f"Counting tokens in {path}")

    def output_tokens(self, outdir: str) -> None:
        for iso_domain in self.unique_tokens:
            print(f"Dumping {iso_domain} in {outdir}")
            os.makedirs(outdir, exist_ok=True)
            txt_file = os.path.join(outdir, iso_domain) + ".txt"
            with open(txt_file, "w", encoding="utf-8") as output:
                output.write(str(self.token_distribution[iso_domain]) + "\n")
                for token in self.unique_tokens[iso_domain]:
                    output.write(token + "\n")

    def find_docpaths(self, inputdir: str) -> Generator[str, None, None]:
        for root, dirs, files in os.walk(inputdir):
            for file in files:
                if file.endswith(".json"):
                    yield os.path.join(root, file)

    def run(self):
        parser = ArgumentParser(description=__doc__)
        parser.add_argument("inputdir", help="Input directory containing json in directories by iso_site/type")
        parser.add_argument("outdir", help="Output directory")
        parser.add_argument("--n-workers", type=int, default=1)
        args = parser.parse_args()

        if args.n_workers == 1:
            for path in self.find_docpaths(args.inputdir):
                self.count_tokens(path)
        else:
            pool = Pool(args.n_workers)
            for path in self.find_docpaths(args.inputdir):
                pool.apply_async(self.count_tokens, path)
            pool.close()
            pool.join()
        self.output_tokens(outdir=args.outdir)


if __name__ == "__main__":
    t = TokenCounter()
    t.run()