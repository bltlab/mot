#! /usr/bin/env python
"""
Takes a directory of extractions json and writes to two output directories:
plaintext paragraph dumps and one sentence tokenized per line.
Skips empty_output.txt files and filtered by language id dir.
"""
import json
import os
from argparse import ArgumentParser

from extraction.document import Document


def plaintextify():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("inputdir", help="Input directory")
    parser.add_argument("outputdir", help="Output directory")
    parser.add_argument("--section", help="Filter articles of a certain section")
    parser.add_argument("--keyword", help="Filter articles of a certain section")
    parser.add_argument("--raw-only", action="store_true")
    parser.add_argument("--tokenized-only", action="store_true")
    args = parser.parse_args()

    raw_outdir = os.path.join(args.outputdir, "raw_paragraphs")
    sents_outdir = os.path.join(args.outputdir, "tokenized_sentences")

    # Walk the input dir
    for subdir in os.listdir(args.inputdir):

        # Completely skip the filtered directory
        if subdir == "lang_id_filtered":
            continue

        insubdir = os.path.join(args.inputdir, subdir)
        # Skip non-directories, like .DS_Store files
        if not os.path.isdir(insubdir):
            continue

        raw_sub = os.path.join(raw_outdir, subdir)
        sents_sub = os.path.join(sents_outdir, subdir)
        if not args.tokenized_only:
            os.makedirs(raw_sub, exist_ok=True)
        if not args.raw_only:
            os.makedirs(sents_sub, exist_ok=True)
        files = os.listdir(insubdir)
        for f in files:
            # This ignores empty_output.txt, which is just a list of urls where we didn't
            # extract anything, but it also covers OS files like .DS_Store
            if not f.endswith(".json"):
                continue
            filepath = os.path.join(insubdir, f)
            with open(filepath, "r", encoding="utf8") as infile:
                json_dict = json.load(infile)
                doc = Document.from_dict(json_dict)
            # Skip document if filtering by section
            if args.section and args.section != doc.section:
                continue
            if args.keyword and args.keyword not in doc.keywords:
                continue
            filename = doc.filename.replace(".html", "") + ".txt"
            raw_path = os.path.join(raw_sub, filename)
            sents_path = os.path.join(sents_sub, filename)
            if not args.tokenized_only:
                with open(raw_path, "w", encoding="utf8") as raw_out:
                    raw_out.write("\n\n".join([doc.title] + doc.paragraphs))
            if not args.raw_only:
                with open(sents_path, "w", encoding="utf8") as sents_out:
                    sents_out.write(doc.title + "\n\n")
                    sents_out.write(
                        "\n\n".join(
                            [
                                "\n".join([" ".join(sent) for sent in paragraph])
                                for paragraph in doc.tokens
                            ]
                        )
                    )


if __name__ == "__main__":
    plaintextify()
