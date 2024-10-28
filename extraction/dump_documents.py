#! /usr/bin/env python
"""
Script to dump all the scraped voa corpus docs stored in mongo to json files.
"""
import json
import os
from argparse import ArgumentParser
from datetime import datetime
from multiprocessing import Process, Pool
from multiprocessing.queues import JoinableQueue
from queue import Queue
from typing import (
    Dict,
    AbstractSet,
    Any,
    Generator,
    Iterable,
    Sequence,
    Optional,
    Tuple,
)

from pymongo import MongoClient
from pymongo.collection import Collection

from extraction.scraper import read_filemap


BYLINES_NOT_ALLOWED = {
    "AP",
    "Associated Press",
    "AFP",
    "Agence France-Presse",
    "Reuters",
}


def stringify_mongovalues(v: Any):
    """
    Mostly the values are fine, but at least datetime needs handled.
    Also need to handle datetimes embedded in dicts, so use recursion to get them.
    """
    if type(v) == datetime:
        return v.isoformat()
    elif type(v) == dict:
        return {key: stringify_mongovalues(value) for key, value in v.items()}
    else:
        return v


def doc_to_dict(doc) -> Dict:
    """Takes whatever the mongo doc is and turns into json serializable dict"""
    ret = {k: stringify_mongovalues(v) for k, v in doc.items() if k != "_id"}
    ret["_id"] = str(doc["_id"])
    return ret


def dump_for_language(
    iso: str,
    outdir: str,
    port: int,
    limit: int = 0,
    content_type: str = "all",
    date_query: Optional[Dict] = None,
):
    for doc in _pull_from_mongodb(
        iso, port, limit, content_type, date_query=date_query
    ):
        lang_dir = os.path.join(outdir, iso)
        os.makedirs(lang_dir, exist_ok=True)
        with open(
            os.path.join(lang_dir, str(doc["_id"]) + ".json"), "w", encoding="utf8"
        ) as outfile:
            json.dump(doc_to_dict(doc), outfile)


def author_allowed(doc: Dict, bad_bylines: AbstractSet):
    """
    Check for authors we can't distribute like AFP and AP.
    Unfortunately utag data has commas and / and sometimes multiple authors.
    Rather than parse the string to a set of authors, just check if substring in the string.
    """
    if not doc["utag_data"]:
        return True
    for bad_byline in bad_bylines:
        if bad_byline in doc["utag_data"].get("byline", ""):
            return False
        for author in doc.get("authors", []):
            if bad_byline in author:
                return False
    return True


def _pull_from_mongodb(
    iso: str,
    port: int = 27200,
    limit: int = 0,
    content_type: str = "all",
    date_query: Optional[Dict] = None,
) -> Generator[Dict, None, None]:
    client = MongoClient(port=port)
    voa_corpus = client.voa_corpus
    collection: Collection = voa_corpus.sitemaps
    query = {"iso": iso, "success": True, "has_ptags": True, "latest": True}
    if content_type != "all":
        query.update({"content_type": content_type})
    if date_query:
        query.update(date_query)

    for doc in collection.find(
        query,
        # limit of 0 is equivalent to no limit
        limit=limit,
    ):
        if not author_allowed(doc, BYLINES_NOT_ALLOWED):
            # Skip authors like AFP, AP, Reuters
            continue
        # Hack around "AP explains: " Title
        if (
            doc["utag_data"]
            and doc["utag_data"]["page_title"]
            and (
                doc["utag_data"]["page_title"].startswith("AP ")
                or doc["utag_data"]["page_title"].startswith("AFP ")
            )
        ):
            continue
        yield doc


def _queue_mongo_docs(
    iso: str,
    queue: JoinableQueue,
    port: int = 27200,
    batchsize: int = 100,
    date_query: Optional[Dict] = None,
):
    batch = []
    doc_count = 0
    batch_count = 0
    for doc in _pull_from_mongodb(iso, port=port, date_query=date_query):
        batch.append(doc_to_dict(doc))
        doc_count += 1
        if len(batch) == batchsize:
            queue.put(batch)
            batch_count += 1
            batch = []

    if batch:
        queue.put(batch)
        batch_count += 1


def enqueue_json_docs(
    queue: Queue,
    languages: Sequence[str],
    port: int = 27200,
    n_processes: int = 1,
    batchsize: int = 100,
    date_query: Optional[Dict] = None,
) -> None:
    pool = Pool(n_processes)
    for language in languages:
        pool.apply(
            _queue_mongo_docs, args=(language, queue, port, batchsize, date_query)
        )


def languages_from_filemap(filemap: str, force_greek: bool = True) -> Tuple[str, ...]:
    print("Reading filemap...")
    filemapdict = read_filemap(filemap)
    languages = sorted(set(filemapdict))
    # XXX: Hack since greek is discontinued in filemaps / sitemaps
    if force_greek:
        languages.append("ell")
    return tuple(languages)


def create_date_query(
    start_date_str: Optional[str] = None, end_date_str: Optional[str] = None
) -> Optional[Dict]:
    if not start_date_str and not end_date_str:
        return None
    start_date = (
        datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None
    )
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None
    if start_date and end_date:
        return {
            "$or": [
                {"date_published": {"$gte": start_date, "$lte": end_date}},
                {"date_published": None},
            ]
        }
    elif start_date and not end_date:
        return {
            "$or": [{"date_published": {"$gte": start_date}}, {"date_published": None}]
        }
    elif end_date and not start_date:
        return {
            "$or": [{"date_published": {"$lte": end_date}}, {"date_published": None}]
        }
    else:
        return None


def dump_documents():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("outdir", help="Dump json documents")
    parser.add_argument("filemap", help="Filemap to get language-iso mapping")
    parser.add_argument("--languages", nargs="+", help="languages to dump")
    parser.add_argument("--n-processes", default=1, type=int)
    parser.add_argument(
        "--doc-cap", default=0, type=int, help="Limit number of docs dumped"
    )
    parser.add_argument(
        "--content-type", choices=["article", "video", "audio", "all"], default="all"
    )
    parser.add_argument("--port", type=int, default=27200)
    parser.add_argument("--start-date", help="%Y-%m-%d")
    parser.add_argument("--end-date", help="%Y-%m-%d")
    args = parser.parse_args()

    # Filemap is just used to get language mapping as early scrapes don't have iso on surface,
    # its burried in sitemap prov
    languages = (
        args.languages if args.languages else languages_from_filemap(args.filemap)
    )
    batches = batch_languages(languages, args.n_processes)
    print(batches)
    date_query = create_date_query(args.start_date, args.end_date)
    for batch in batches:
        processes = [
            Process(
                target=dump_for_language,
                args=(
                    lang,
                    args.outdir,
                    args.port,
                    args.doc_cap,
                    args.content_type,
                    date_query,
                ),
            )
            for lang in batch
        ]
        for p in processes:
            p.start()

        for p in processes:
            p.join()


def batch_languages(languages: Iterable[str], n_processes: int):
    batches = []
    batch = []
    for i, lang in enumerate(languages):
        batch.append(lang)
        if i % n_processes == n_processes - 1:
            batches.append(batch)
            batch = []
    if batch:
        batches.append(batch)
    return batches


if __name__ == "__main__":
    dump_documents()
