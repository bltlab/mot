from pymongo import MongoClient
from pymongo.collection import Collection


ARCHIVEDOTORG = "archive.org"


def get_sitemap_collection(port: int = 27200) -> Collection:
    client = MongoClient(port=port)
    voa_corpus = client.voa_corpus
    return voa_corpus.sitemaps
