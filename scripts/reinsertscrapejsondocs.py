import json
import os
from argparse import ArgumentParser
from datetime import datetime
from multiprocessing import Pool
from typing import Sequence

from extraction.utils import get_sitemap_collection


def open_and_insert(filepaths: Sequence[str], port=27200) -> None:
    collection = get_sitemap_collection(port=port)
    docs_to_insert = []
    for filepath in filepaths:
        with open(filepath, "r", encoding="utf8") as f:
            doc = json.load(f)
            doc["date_published"] = datetime.strptime(doc["date_published"], "%Y-%m-%dT%H:%M:%S")
        docs_to_insert.append(doc)
        if len(docs_to_insert) >= 100:
            collection.insert_many(docs_to_insert)
            docs_to_insert = []
    if docs_to_insert:
        collection.insert_many(docs_to_insert)


def reinsert_scraped_json_docs():
    parser = ArgumentParser()
    parser.add_argument("inputdir")
    parser.add_argument("--port", default=27200, type=int)
    parser.add_argument("--n-workers", default=1, type=int)
    args = parser.parse_args()

    pool = Pool(args.n_workers)
    batches = []
    batch = []
    for filename in os.listdir(args.inputdir):
        filepath = os.path.join(args.inputdir, filename)
        batch.append(filepath)
        if len(batch) >= 1000:
            batches.append(batch)
            batch = []
    if batch:
        batches.append(batch)

    # Multiprocessing doesn't work yet, doing something wrong,
    # but fast enough on small datasets i don't care
    for batch in batches:
        # open_and_insert(batch, scrapes_collection)
        pool.apply_async(open_and_insert, (batch,))
    pool.close()
    pool.join()


if __name__ == "__main__":
    reinsert_scraped_json_docs()
