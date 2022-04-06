from pymongo import MongoClient
from pymongo.collection import Collection


ARCHIVEDOTORG = "archive.org"


def get_sitemap_collection(port: int = 27200) -> Collection:
    client = MongoClient(port=port)
    voa_corpus = client.voa_corpus
    return voa_corpus.sitemaps


SPACE_CHARS = {
    '\u1361',  # ETHIOPIC WORDSPACE
    '\u200b',  # ZERO WIDTH SPACE
    '\u2408',  # SYMBOL FOR BACKSPACE
    '\u2420',  # SYMBOL FOR SPACE
    '\u303f',  # IDEOGRAPHIC HALF FILL SPACE
    '\ufeff'   # ZERO WIDTH NO-BREAK SPACE
}
SPACE_CHARS_STR = "".join(SPACE_CHARS)
