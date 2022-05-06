from datetime import datetime
from typing import Dict, Optional

from pymongo import MongoClient
from pymongo.collection import Collection


ARCHIVEDOTORG = "archive.org"


def get_sitemap_collection(port: int = 27200) -> Collection:
    client = MongoClient(port=port)
    voa_corpus = client.voa_corpus
    return voa_corpus.sitemaps


SPACE_CHARS = {
    "\u1361",  # ETHIOPIC WORDSPACE
    "\u200b",  # ZERO WIDTH SPACE
    "\u2408",  # SYMBOL FOR BACKSPACE
    "\u2420",  # SYMBOL FOR SPACE
    "\u303f",  # IDEOGRAPHIC HALF FILL SPACE
    "\ufeff",  # ZERO WIDTH NO-BREAK SPACE
}
SPACE_CHARS_STR = "".join(SPACE_CHARS)


def get_publication_date_from_utag(utag_data: Dict) -> Optional[str]:
    """
    Returns a datetime in format "2016-03-15T00:00:00"
    """
    # pub_year, pub_month, pub_day, pub_hour,  pub_min
    pub_year = utag_data.get("pub_year")
    pub_month = utag_data.get("pub_month")
    pub_day = utag_data.get("pub_day")
    pub_hour = utag_data.get("pub_hour")
    pub_min = utag_data.get("pub_minute")
    pub_date = None
    try:
        if pub_year and pub_month and pub_day:
            if pub_min and pub_hour:
                pub_date = datetime.fromisoformat(
                    f"{pub_year}-{pub_month}-{pub_day}:{pub_hour}:{pub_min}"
                )
            else:
                pub_date = datetime.fromisoformat(f"{pub_year}-{pub_month}-{pub_day}")
    except ValueError as e:
        # Exception for days that aren't possible in months (possibly caused by leap year)
        if pub_year and pub_month:
            # Hack: still want to have the year and month if possible so hack date
            pub_date = datetime.fromisoformat(f"{pub_year}-{pub_month}-01")
    if pub_date:
        return pub_date.isoformat()
    else:
        return pub_date
