from argparse import ArgumentParser

from pymongo import MongoClient
from pymongo.collection import Collection

from extraction.scraper import read_filemap, pages_from_sitemaps

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("filemap")
    parser.add_argument("sitemapdir")
    parser.add_argument(
        "--languages", nargs="+", help="Languages to fix using iso-3 codes"
    )
    args = parser.parse_args()

    client = MongoClient(port=27200)
    voa_corpus = client.voa_corpus
    sitemap_collection: Collection = voa_corpus.sitemaps

    sitemaps_by_language = read_filemap(args.filemap)
    for lang in sitemaps_by_language:
        if not args.languages or lang in set(args.languages):
            pages_generator = pages_from_sitemaps(
                sitemaps_by_language[lang], args.sitemapdir
            )
            url_to_page = {p.url: p for p in pages_generator}
            for doc in sitemap_collection.find({"iso": lang}):
                page = url_to_page.get(doc["url"], None)
                if page:
                    sitemap_collection.find_one_and_update(
                        {"_id": doc["_id"]},
                        {"$set": {"sitemap_prov": page.sitemap_prov.to_dict()}},
                    )
