#! /usr/bin/env python
"""
Script to check sentence segmentation
"""

import os
import json
from argparse import ArgumentParser
from collections import defaultdict, Counter
from multiprocessing import Pool
from typing import Generator


class SentenceCounter:
    def __init__(self) -> None:
        # Distribution of sentence lengths by language/domain
        self.sentence_chars_count: defaultdict[str, Counter[str]] = defaultdict(Counter)
        # Unique sentences by language/domain and sentence length
        self.sentences_by_length: defaultdict[str, defaultdict[int, set]] = defaultdict(defaultdict)
        # Measure of rate that paragraphs are segmented into sentences by language
        # TODO: Add
        self.division_rate: defaultdict[str, float] = defaultdict()

    def count_sentence_length(self, path: str) -> None:
        """Counts and stores sentences and sentence lengths in each json file"""
        with open(path) as file:
            iso_domain = path.split("/")[1]
            data = json.load(file)
            sentences = data.get('sentences')
            n_sentences = len(sentences)
            # paragraphs = data.get('paragraphs')
            n_paragraphs = data.get('n_paragraphs')
            # url = data.get('url')
            # self.division_rate[iso_domain] += n_sentences
            for sentence in sentences:
                self.sentence_chars_count[iso_domain][len(sentence)] += 1
                if len(sentence) not in self.sentences_by_length[iso_domain]:
                    self.sentences_by_length[iso_domain][len(sentence)] = set()
                self.sentences_by_length[iso_domain][len(sentence)].add(sentence)
        print(f"Counting sentences in {path}")

    def sort_counts_chars(self):
        """Sorts distribution of sentence lengths by length"""
        for language, count in self.sentence_chars_count.items():
            self.sentence_chars_count[language] = dict(sorted(count.items()))
        return dict(sorted(self.sentence_chars_count.items()))

    def sort_counts_sentences(self):
        """Sorts sets of unique sentences by length"""
        for language, count in self.sentences_by_length.items():
            self.sentences_by_length[language] = dict(sorted(count.items()))
        return self.sentences_by_length

    def dump_sentences_by_length(self, outdir: str) -> None:
        """Writes unique sentences by length to txt file by language"""
        for iso_domain in self.sort_counts_sentences():
            print(f"Dumping {iso_domain} in {outdir}")
            os.makedirs(outdir, exist_ok=True)
            txt_file = os.path.join(outdir, iso_domain) + ".txt"
            with open(txt_file, "w", encoding="utf-8") as output:
                output.write(str(self.sort_counts_chars()[iso_domain]) + "\n")
                for sentence_length in self.sentences_by_length[iso_domain]:
                    for sentence in self.sentences_by_length[iso_domain][sentence_length]:
                        output.write(str(sentence_length) + ": " + sentence + "\n")

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
                self.count_sentence_length(path)
        else:
            pool = Pool(args.n_workers)
            for path in self.find_docpaths(args.inputdir):
                pool.apply_async(self.count_sentence_length, path)
            pool.close()
            pool.join()
        # TODO: Move
        self.dump_sentences_by_length(args.outdir)


if __name__ == "__main__":
    s = SentenceCounter()
    s.run()
