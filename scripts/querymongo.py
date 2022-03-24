"""
Quick script to query mongo documents.
"""
from argparse import ArgumentParser
from datetime import datetime
from typing import Sequence

from pymongo import MongoClient
from pymongo.collection import Collection

from extraction.dump_documents import create_date_query
from extraction.scraper import read_filemap
from extraction.utils import ARCHIVEDOTORG


def print_doc_counts(
    iso_codes: Sequence[str],
    collection: Collection,
):
    print(f"iso\tarticle\tlatest\tlatest_archive\tarchive\traw.count")
    for iso in iso_codes:
        article_count = collection.count_documents(
            {"iso": iso, "content_type": "article", "latest": True, "success": True}
        )
        latest_count = collection.count_documents(
            {"iso": iso, "success": True, "latest": True}
        )
        latest_and_archive_count = collection.count_documents(
            {
                "iso": iso,
                "success": True,
                "latest": True,
                "sitemap_prov.filename": ARCHIVEDOTORG,
            }
        )
        archive_count = collection.count_documents(
            {"iso": iso, "success": True, "sitemap_prov.filename": ARCHIVEDOTORG}
        )
        raw_count = collection.count_documents({"iso": iso})
        if raw_count > 0:
            print(
                iso,
                article_count,
                latest_count,
                latest_and_archive_count,
                archive_count,
                raw_count,
                sep="\t",
            )


def remove_languages(
    iso_codes: Sequence[str],
    collection: Collection,
):
    for iso in iso_codes:
        collection.delete_many({"iso": iso})


def check_distinct_counts(collection: Collection):
    print("Estimated doc count: ", collection.estimated_document_count())
    for result in collection.aggregate(
        [
            {"$group": {"_id": "$canonical_link"}},
            {"$group": {"_id": 1, "count": {"$sum": 1}}},
        ],
        allowDiskUse=True,
    ):
        print("Distinct doc count: ", result["count"])


def archive_counts(
    iso_codes: Sequence[str],
    collection: Collection,
):
    print("Distinct counts of success archive pages deduplicated by canonical link")
    for iso in iso_codes:
        for result in collection.aggregate(
            [
                {"$match": {"iso": iso, "success": True, "content_type": "article"}},
                {
                    "$group": {
                        "_id": "$canonical_link",
                        "num_docs": {"$sum": 1},
                        "prov": {"$push": "$sitemap_prov.filename"},
                    }
                },
                {"$match": {"num_docs": {"$lte": 1}, "prov": ARCHIVEDOTORG}},
                {"$group": {"_id": 1, "count": {"$sum": 1}}},
            ],
            allowDiskUse=True,
        ):
            print(f"{iso}", result["count"])


def query():
    parser = ArgumentParser()
    parser.add_argument("--canon-url")
    parser.add_argument("--filemap")
    parser.add_argument("--counts-by-lang", nargs="+")
    parser.add_argument("--all-langs", action="store_true")
    parser.add_argument("--errors-by-lang", nargs="+")
    parser.add_argument("--remove-languages", nargs="+")
    parser.add_argument("--distinct-canonurl", action="store_true")
    parser.add_argument("--port", type=int, default=27200)
    parser.add_argument("--single-doc", action="store_true")
    parser.add_argument("--archive-counts", action="store_true")
    parser.add_argument("--query-by-date", action="store_true")
    parser.add_argument("--start-date", help="%Y-%m-%d")
    parser.add_argument("--end-date", help="%Y-%m-%d")
    args = parser.parse_args()

    client = MongoClient(port=args.port)
    voa_corpus = client.voa_corpus
    sitemap_collection: Collection = voa_corpus.sitemaps

    if args.single_doc:
        for doc in sitemap_collection.find():
            # hack this whenever you want to see something else
            print(doc.keys())
            break

    if args.canon_url:
        for doc in sitemap_collection.find({"canonical_link": args.canon_url.strip()}):
            print(doc["url"])

    if args.remove_languages:
        choice = input(
            f"You are about to remove {args.remove_languages}.\n Are you sure you want to (y/n)?\n"
        )
        if choice.lower().startswith("y"):
            remove_languages(args.remove_languages, sitemap_collection)

    if args.distinct_canonurl:
        check_distinct_counts(sitemap_collection)
    if args.all_langs and args.counts_by_lang:
        if not args.filemap:
            raise ValueError("Need filemap to do counts of all languages")
        filemapdict = read_filemap(args.filemap)
        print_doc_counts(list(filemapdict), sitemap_collection)
    elif args.counts_by_lang:
        print_doc_counts(args.counts_by_lang, sitemap_collection)
    elif args.archive_counts:
        if args.all_langs:
            if not args.filemap:
                raise ValueError("Need filemap to do counts of all languages")
            filemapdict = read_filemap(args.filemap)
            archive_counts(list(filemapdict), sitemap_collection)
        else:
            archive_counts(args.counts_by_lang, sitemap_collection)

    if args.query_by_date:
        date_query = create_date_query(args.start_date, args.end_date)
        print(date_query)
        for doc in sitemap_collection.find(date_query):
            # print(doc.keys())
            pub_date = doc.get("date_published")
            if pub_date:
                print(pub_date)
    # TODO: view errors of docs that failed by language


if __name__ == "__main__":
    query()
