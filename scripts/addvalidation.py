from argparse import ArgumentParser

from pymongo import MongoClient
from collections import OrderedDict

from pymongo.collection import Collection


def add_and_test_validation(port: int = 27200):
    client = MongoClient(port=port)
    voa_corpus = client.voa_corpus

    # TODO define schema for validation
    vexpr = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "url",
                "sitemap_prov",
                "success",
                "language",
                "iso",
                "time_retrieved",
                "error_message",
                "sitemap_last_modified",
            ],
            "additionalProperties": True,
            "properties": {
                "_id": {},
                "url": {
                    "bsonType": "string",
                    "description": "required string url of webpage",
                },
                "sitemap_prov": {
                    "bsonType": "object",
                    "additionalProperties": True,
                    "description": "must be a string and is not required",
                },
                "time_retrieved": {
                    "bsonType": "date",
                    "description": "datetime the article was retrieved or attempted retrieval time",
                },
                "success": {
                    "bsonType": "bool",
                    "description": "whether the page was retrieved successfully",
                },
                "sitemap_last_modified": {
                    "bsonType": ["date", "null"],
                    "description": "datetime when sitemap entry last modified",
                },
                "language": {"bsonType": "string", "description": "language"},
                "iso": {"bsonType": "string", "description": "iso 3-letter code"},
                "error_message": {
                    "bsonType": ["string", "null"],
                    "description": "error message for failed scrape",
                },
                "canonical_link": {
                    "bsonType": ["string", "null"],
                    "description": "string for canonical url",
                },
                "content_type": {
                    "bsonType": ["string", "null"],
                    "description": "type of article if parsed from utag data",
                },
                "title": {
                    "bsonType": ["string", "null"],
                    "description": "Title extracted from html metadata",
                },
                "changefreq": {
                    "bsonType": ["string", "null"],
                    "description": "Change frequency",
                },
                "priority": {
                    "bsonType": ["string", "null"],
                    "description": "value from sitemaps assigning priority",
                },
                "description": {
                    "bsonType": ["string", "null"],
                    "description": "description retrieved from html metadata",
                },
                "keywords": {
                    "bsonType": ["array"],
                    "description": "array of keywords from metadata",
                },
                "authors": {
                    "bsonType": ["array"],
                    "minItems": 0,
                    "description": "authors from html, must be array as are multiple, empty array if none",
                },
                "utag_data": {
                    "bsonType": "object",
                    "additionalProperties": True,
                    "description": "utag data from html is a dictionary of unknown values",
                },
                "application_ld_json": {
                    "bsonType": "object",
                    "additionalProperties": True,
                    "description": "dictionary of application ld json found in html",
                },
                "has_ptags": {
                    "bsonType": "bool",
                    "description": "whether or not the document has ptags in the html",
                },
                "original_html": {
                    "bsonType": ["string", "null"],
                    "description": "original html scraped",
                },
                "date_published": {
                    "bsonType": ["date", "null"],
                    "description": "datetime published if available in html",
                },
                "date_modified": {
                    "bsonType": ["date", "null"],
                    "description": "datetime the page was modified from html",
                },
                "latest": {
                    "bsonType": "bool",
                    "description": "whether the page is most recent update",
                },
            },
        }
    }

    cmd = OrderedDict(
        [("collMod", "sitemaps"), ("validator", vexpr), ("validationLevel", "moderate")]
    )

    voa_corpus.command(cmd)


def add_validation():
    parser = ArgumentParser()
    parser.add_argument("--port", type=int, default=27200)
    args = parser.parse_args()

    add_and_test_validation(args.port)


if __name__ == "__main__":
    add_validation()
