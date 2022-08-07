#! /usr/bin/env python
"""
Script to get ending characters from a file with one paragraph per line.
Prints a list of most common paragraph ending punctuation, the unicode of the character
and number of occurrences.
"""
from argparse import ArgumentParser
from collections import Counter


def find_ending_punctuation():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("input_file")
    args = parser.parse_args()

    char_counter = Counter()

    with open(args.input_file, "r", encoding="utf8") as file:
        for line in file:
            ending_char = line.strip()[-1]
            char_counter[ending_char] += 1

    print(f"{'Char':7s} {'Unicode':9s} {'Count':5s}")
    for char, count in char_counter.most_common():
        print(f"{char:7s} {ord(char):9s} {count:5.0f}")


if __name__ == "__main__":
    find_ending_punctuation()
