import json
import os
from argparse import ArgumentParser
from datetime import datetime
from multiprocessing.context import Process
from multiprocessing import JoinableQueue
from typing import Generator

from attr import attrs
from bson import ObjectId
from pymongo import UpdateOne

from extraction.utils import get_sitemap_collection


@attrs(frozen=True, auto_attribs=True)
class DocItem:
    docid: ObjectId
    time_retrieved: datetime


def get_files(prevdir: str) -> Generator[DocItem, None, None]:
    for root, dirs, files in os.walk(prevdir):
        for f in files:
            docid = ObjectId(f.rstrip(".json"))
            with open(os.path.join(root, f), "r", encoding="utf8") as inputfile:
                doc = json.load(inputfile)
                new_time = datetime.strptime(
                    doc["time_retrieved"], "%d-%b-%Y (%H:%M:%S.%f)"
                )
            yield DocItem(docid, new_time)


def create_update_op(docitem: DocItem):

    return UpdateOne(
        {"_id": docitem.docid},
        {
            "$set": {
                "time_retrieved": docitem.time_retrieved,
            }
        },
    )


def _work(queue: JoinableQueue, worker_id: int) -> None:
    print(f"Starting worker {worker_id}")
    collection = get_sitemap_collection()
    write_operations = []
    while True:
        i, batch = queue.get()
        for docitem in batch:
            update_operation = create_update_op(docitem)
            if update_operation:
                write_operations.append(update_operation)
            if len(write_operations) >= 20:
                collection.bulk_write(write_operations)
                write_operations = []
        # Write the rest of operations
        if write_operations:
            collection.bulk_write(write_operations)
            write_operations = []
        queue.task_done()


def fix_retrieved():
    """
    Getting time retrieved from previous scrapes dump
    and re adding them to the database where the time retrieved was overwritten accidentally
    in a previous timestamp formatting update.
    This won't get every document, but should get most of them.
    """
    parser = ArgumentParser()
    parser.add_argument("prevdir", help="Dir with previous scrapes")
    parser.add_argument("--n-workers", type=int, default=1)
    parser.add_argument("--batchsize", type=int, default=100)
    args = parser.parse_args()

    # Create a queue for batches of IDs
    queue: JoinableQueue = JoinableQueue()

    workers = [Process(target=_work, args=(queue, i)) for i in range(args.n_workers)]
    for worker in workers:
        worker.daemon = True
        worker.start()

    print(f"Loading IDs from {args.prevdir}")
    article_count = 0
    batch_count = 0

    batch = []
    for doc_item in get_files(args.prevdir):
        batch.append(doc_item)
        article_count += 1
        if len(batch) == args.batchsize:
            queue.put((batch_count, batch))
            batch_count += 1
            batch = []

    # Add final batch
    if batch:
        queue.put((batch_count, batch))
        batch_count += 1

    print(f"Added {article_count} articles to queue in {batch_count} batches")
    queue.join()
    print("All queue tasks complete")


if __name__ == "__main__":
    fix_retrieved()
