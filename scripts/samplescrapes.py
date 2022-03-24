#! /usr/bin/env python
"""
Copys directory structure and copies a sample of the files in
the input director to the output directory.
"""
import os
import shutil
from argparse import ArgumentParser
import random
from os.path import basename

random.seed(0)


def samplescrapes():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("indir")
    parser.add_argument("outdir")
    parser.add_argument("--n", default=100, type=int, help="Number of docs to sample")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    # Ensure indir has trailing slash
    indir = os.path.join(args.indir, "")
    for dirpath, dirnames, filenames in os.walk(indir):
        structure = os.path.join(args.outdir, dirpath[len(indir) :])
        if not os.path.isdir(structure):
            os.makedirs(structure, exist_ok=True)
        if len(filenames) < args.n:
            samplesize = len(filenames)
        else:
            samplesize = args.n
        samplefiles = random.sample(filenames, samplesize)
        for f in samplefiles:
            shutil.copyfile(os.path.join(dirpath, f), os.path.join(structure, f))


if __name__ == "__main__":
    samplescrapes()
