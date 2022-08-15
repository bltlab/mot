#! /usr/bin/env python
"""
Script to check sentence segmentation
"""

import os
import json
from argparse import ArgumentParser
from collections import defaultdict, Counter
from heapq import heappushpop
from multiprocessing import Pool
from typing import Generator, NamedTuple, Dict, Optional, List, Tuple

html_template = """
<!DOCTYPE html>
<meta charset="UTF-8">
<html>
<body>

*INSERT_CONTENT_HERE*

</body>
</html>"""


class Result(NamedTuple):
    is_article: bool
    is_segmented: bool
    sentence_endings: Counter
    sents_by_length: Dict
    iso: str
    segmentation_rate: Optional[float]


class SentenceCounter:
    def __init__(self, top_k_longest: int, num_top_sent_endings: int) -> None:
        # n_sentences / n_paragraphs sum for all articles of a language
        self.segmentation_rate_sum_by_lang: defaultdict[str, float] = defaultdict(float)
        self.article_count_by_lang: defaultdict[str, int] = defaultdict(int)
        self.num_segmented: defaultdict[str, int] = defaultdict(int)
        self.num_unsegmented: defaultdict[str, int] = defaultdict(int)
        # Things like acronyms and titles like ACB. or Mr. or Dkt.
        self.likely_bad_splits: defaultdict[str, Counter] = defaultdict(Counter)
        self.top_k_longest: int = top_k_longest
        self.longest_sents: defaultdict[str, List[Tuple[int, str]]] = defaultdict(list)
        self.sentence_lengths_frequency: defaultdict[str, Counter] = defaultdict(Counter)
        self.num_top_sent_endings: int = num_top_sent_endings

    def write_all_results(self, outdir: str):
        for iso in self.segmentation_rate_sum_by_lang:
            outpath = os.path.join(outdir, iso + ".html")
            seg_rate = self.segmentation_rate_sum_by_lang[iso] / self.article_count_by_lang[iso]
            long_sents = "\n".join([f"<p>{sent[1]}</p>" for sent in self.longest_sents[iso]])
            sentence_endings = "\n".join(
                [
                    f"<p>{sent} ({count}) </p>"
                    for sent, count in self.likely_bad_splits[iso].most_common(self.num_top_sent_endings)
                ]
            )
            content = f"<h1>Results for {iso}</h1>\n" \
                      f"<p><b>avg segmentation rate</b>: {seg_rate:.2f}</p>\n" \
                      f"<p><b>Segmented</b>: {self.num_segmented[iso]}</p>\n" \
                      f"<p><b>Unsegmented</b>: {self.num_unsegmented[iso]}</p>\n" \
                      f"<br>\n" \
                      f"<h2>Longest N sentences:</h2>\n" \
                      f"{long_sents}\n" \
                      f"<br>\n" \
                      f"{sentence_endings}\n"

            with open(outpath, 'w', encoding='utf8') as outfile:
                outfile.write(html_template.replace("*INSERT_CONTENT_HERE*", content))

    def process_result(self, result: Result):
        if result.segmentation_rate is not None:
            self.segmentation_rate_sum_by_lang[result.iso] += result.segmentation_rate
        if result.is_article:
            self.article_count_by_lang[result.iso] += 1
        if result.is_segmented:
            self.num_segmented[result.iso] += 1
        else:
            self.num_unsegmented[result.iso] += 1

        self.likely_bad_splits[result.iso] += result.sentence_endings

        for length, sent in result.sents_by_length.items():
            if len(self.longest_sents[result.iso]) < self.top_k_longest:
                self.longest_sents[result.iso].append((length, sent))
            else:
                heappushpop(self.longest_sents[result.iso], (length, sent))
            self.sentence_lengths_frequency[result.iso][length] += 1


def find_docpaths(inputdir: str) -> Generator[str, None, None]:
    for root, dirs, files in os.walk(inputdir):
        for file in files:
            if file.endswith(".json"):
                yield os.path.join(root, file)


def count_docs(path: str) -> Result:
    with open(path, 'r', encoding='utf8') as file:
        doc = json.load(file)
        iso = doc.get("predicted_language")
        n_paragraphs = doc.get("n_paragraphs")
        n_sentences = doc.get("n_sentences")
        segmentation_rate = n_paragraphs / n_sentences if n_paragraphs and n_sentences else None
        sentence_endings = Counter()
        sentences = doc.get("sentences", [[]])
        for paragraph in sentences:
            for s in paragraph:
                space_separated_chunks = s[-5:].split()
                if space_separated_chunks:
                    ending = space_separated_chunks[-1]
                    sentence_endings[ending] += 1
        return Result(
            is_article = doc.get("content_type") == "article",
            segmentation_rate = segmentation_rate,
            is_segmented = n_sentences and segmentation_rate is not None and segmentation_rate < 1,
            sentence_endings=sentence_endings,
            iso=iso,
            sents_by_length={len(s): s  for paragraph in sentences for s in paragraph}
        )


def run():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputdir",
        help="Input directory containing json in directories by iso_site/type"
    )
    parser.add_argument("outdir", help="Output directory")
    parser.add_argument("--n-workers", type=int, default=1)
    args = parser.parse_args()

    counter = SentenceCounter(top_k_longest=15, num_top_sent_endings=20)

    if args.n_workers == 1:
        for path in find_docpaths(args.inputdir):
            result = count_docs(path)
            if result.is_article:
                counter.process_result(result)
    else:
        with Pool(args.n_workers) as pool:
            for result in pool.imap_unordered(
                    count_docs, find_docpaths(args.inputdir), chunksize=100
            ):
                if result.is_article:
                    counter.process_result(result)
    counter.write_all_results(args.outdir)


if __name__ == "__main__":
    run()
