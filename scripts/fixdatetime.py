#! /usr/bin/env python
from argparse import ArgumentParser
from datetime import datetime
from typing import Optional, Union

from pymongo import MongoClient
from pymongo.collection import Collection

## This one doesn't work with lower than 4.2 which we're using on lignos04 right now...
## Keeping it here commented out in case I need it and we update to more recent
# def update_timestamps(collection: Collection):
#     collection.update_many(
#         {},
#         [
#             { "$set": {
#                 "timestamp": {
#                         "$dateFromString": {"dateString": "$timestamp"}
#                             },
#                 "sitemap_prov.sitemap.timestamp": {"$dateFromString": {"dateString": "$sitemap_prov.sitemap.timestamp"}}
#                         }
#                     },
#         ]
#     )


def update_times(collection: Collection):
    for doc in collection.find():
        retrieved = doc.get("time_retrieved")
        timestamp = doc.get("timestamp")
        embedded_timestamp = doc["sitemap_prov"]["sitemap"]["timestamp"]

        if type(retrieved) is not datetime:
            retrieved = fix_time_retrieved(retrieved)
        if type(timestamp) is not datetime:
            timestamp = fix_timestamp(timestamp)
        if type(embedded_timestamp) is not datetime:
            embedded_timestamp = fix_timestamp(embedded_timestamp)
        collection.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "time_retrieved": retrieved,
                    "timestamp": timestamp,
                    "sitemap_prov.sitemap.timestamp": embedded_timestamp,
                }
            },
        )


def rundatetime():
    parser = ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--fix-dates", action="store_true")
    args = parser.parse_args()

    client = MongoClient(port=27200)
    voa_corpus = client.voa_corpus
    sitemap_collection: Collection = voa_corpus.sitemaps

    if args.fix_dates:
        update_times(sitemap_collection)

    elif args.debug:
        sitemap_collection.count_documents({"timestamp": {"$type": "date"}})

        # for doc in sitemap_collection.find({"timestamp": {"$gte": datetime.strptime("2021-06-10T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
        # "$lt": datetime.strptime("2021-07-10T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")}}):
        #     print(doc["url"])
        #     print(doc["timestamp"])

        # for doc in sitemap_collection.find(
        #     {"iso": "fra", "success": True, "has_ptags": True, "content_type": "article"},
        #     limit=0,
        # ):
        #     print(doc["time_retrieved"])
        #     print(doc["timestamp"])
        #     # print(doc["timestamp"])
        #     # print(doc["language"])
        #     print(doc["sitemap_prov"]["sitemap"]["timestamp"])
        #     print(doc.keys())
    else:
        raise ValueError("Choose either --debug or --fix-dates")


# There are two datetime formats that need converted from string
def fix_timestamp(s: Optional[str]) -> Optional[Union[str, datetime]]:
    """Fix timestamps of format 2021-06-08T16:03:48.75Z"""
    # In the case that the sitemaps didn't have a timestamp just pass along what they had
    if not s or s == "None":
        return s
    # Hacking to get format correct. Trim the millisecond decimals if needed
    if len(s) >= len("2021-06-13T02:05:46.170195Z"):
        s = s[: len("2021-06-13T02:05:46.170195")] + "Z"
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")
    except:
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        except:
            return datetime.fromisoformat(s)


def fix_time_retrieved(s: str) -> Optional[Union[str, datetime]]:
    """
    Fixes timestamps of the format: 15-Jul-2021 (22:29:25.643316)
    """
    if not s or s == "None":
        return s
    return datetime.strptime(s, "%d-%b-%Y (%H:%M:%S.%f)")


if __name__ == "__main__":
    rundatetime()
