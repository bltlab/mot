import os
from argparse import ArgumentParser
from typing import List, Generator
import xml.etree.ElementTree as ET


# Amharic/Tigrinya Punctuation
# ፨    U+1368   Paragraph break
# ።     U+1362   Full stop
# ፡     U+1361   Wordspace
# ፠    U+1360   Section mark
# ፣     U+1363   Comma
# ፧     U+1367   Question mark
# ፤     U+1364   Semicolon
# ፥     U+1365   Colon
# ፦     U+1366   Preface Colon

GEEZ_PUNCTUATION = {
    "\u1368",
    "\u1362",
    "\u1361",
    "\u1360",
    "\u1363",
    "\u1367",
    "\u1364",
    "\u1365",
    "\u1366",
}


def walk_dir(indir: str) -> Generator[str, None, None]:
    for root, dirs, files in os.walk(indir):
        for file in files:
            yield os.path.join(root, file)


def read_ner_sentences(filepath: str) -> List[str]:
    """
    Read NER conll format with one token per line and the entity type
    Sentences separated by blank line.
    """
    with open(filepath, "r", encoding="utf8") as f:
        sents = []
        sent = []
        for line in f:
            if not line.strip():
                if sent:
                    sents.append(" ".join(sent))
                    sent = []
                continue
            else:
                fields = line.rstrip().split()
                if fields[0] in GEEZ_PUNCTUATION:
                    sent.append(sent.pop(-1) + fields[0])
                else:
                    sent.append(fields[0])
        if sent:
            sents.append(" ".join(sent))
        return sents


def read_ud_sentences(filepath: str) -> List[str]:
    """
    Read UD sentences as they are. Not worrying about proper detokenization.
    """
    SENT_PREFIX = "# text = "
    with open(filepath, "r", encoding="utf8") as f:
        sents = []
        for line in f:
            if line.startswith(SENT_PREFIX):
                sents.append(line[len(SENT_PREFIX) :].strip())
        return sents


def read_xml_sentences(filepath: str) -> List[str]:
    """
    Reads from xml format to sentence strings. Attempts to detokenize punctuation.
    Currently only supports Ge'ez punctuation.
    """
    tree = ET.parse(filepath)
    root = tree.getroot()
    sents = []
    sent = []
    for elem in root.iter():
        if elem.tag == "w":
            if elem.text in GEEZ_PUNCTUATION:
                sent.append(sent.pop(-1) + elem.text)
            else:
                sent.append(elem.text)
        elif elem.tag == "s":
            sents.append(" ".join(sent))
            sent = []
    return sents


def main():
    parser = ArgumentParser()
    parser.add_argument("indir")
    parser.add_argument("outfile")
    parser.add_argument("--doc-type", choices=["xml", "ner", "ud"])
    args = parser.parse_args()
    sents = []
    if args.doc_type == "ner":
        for filepath in walk_dir(args.indir):
            sents.extend(read_ner_sentences(filepath))
    elif args.doc_type == "ud":
        for filepath in walk_dir(args.indir):
            sents.extend(read_ud_sentences(filepath))
    elif args.doc_type == "xml":
        for filepath in walk_dir(args.indir):
            sents.extend(read_xml_sentences(filepath))
    else:
        raise ValueError(f"Invalid doc type {args.doc_type}")

    with open(args.outfile, "w", encoding="utf8") as out:
        for sent in sents:
            print(sent, file=out)


if __name__ == "__main__":
    main()
