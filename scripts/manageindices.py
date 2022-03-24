from argparse import ArgumentParser

from pymongo import MongoClient, HASHED, DESCENDING
from pymongo.collection import Collection

from extraction.scraper import create_scrape_indices


def manage_indices():
    parser = ArgumentParser()
    parser.add_argument("--list-indices", action="store_true")
    parser.add_argument("--drop-index")
    parser.add_argument(
        "--create-index",
        action="store_true",
        help="Actual index to create is hard coded for now",
    )
    parser.add_argument("--port", type=int, default=27200)
    args = parser.parse_args()

    client = MongoClient(port=27200)
    voa_corpus = client.voa_corpus
    sitemap_collection: Collection = voa_corpus.sitemaps

    if args.list_indices:
        for index in sitemap_collection.list_indexes():
            print(index)

    if args.drop_index:
        sitemap_collection.drop_index(args.drop_index)

    if args.create_index:
        create_scrape_indices(sitemap_collection)


if __name__ == "__main__":
    manage_indices()
