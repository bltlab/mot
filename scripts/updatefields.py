"""
Updates fields for a given language in the database
"""
from argparse import ArgumentParser
from datetime import datetime
from multiprocessing.context import Process
from multiprocessing import JoinableQueue
from typing import Optional, Sequence, Dict

import json5
from bs4 import BeautifulSoup, Tag
from bson import ObjectId
from pymongo import UpdateOne


from extraction.scraper import extract_utag_data
from extraction.utils import get_sitemap_collection


def update_fields():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--language_code", help="Update all the docs with iso code")
    parser.add_argument("--language", help="Update all the docs with language")
    parser.add_argument("--update-utag", action="store_true", help="Update utag data")
    parser.add_argument(
        "--update-iso", action="store_true", help="Update iso for a language"
    )
    parser.add_argument(
        "--update-lang-by-sitename",
        help="Update all the language and iso for a sitename",
    )
    parser.add_argument(
        "--update-metadata",
        action="store_true",
        help="Update authors, date published/modified",
    )
    parser.add_argument("--n-workers", type=int, default=1)
    parser.add_argument("--dump-ids", help="Path to file to dump all mongo ids")
    parser.add_argument("--ids-path")
    parser.add_argument("--rename-field")
    parser.add_argument("--new-name")
    parser.add_argument("--remove-field")
    parser.add_argument("--add-latest", action="store_true")
    parser.add_argument("--port", type=int, default=27200)
    args = parser.parse_args()

    # TODO number of arguments is becoming too much, consider click style args or separate scripts
    if args.update_lang_by_sitename and not (args.language_code and args.language):
        raise ValueError(
            "Must specify language and language code when updating by language by sitename"
        )

    if args.update_utag and args.language_code:
        print(f"Updating utag for {args.language_code}")
        sitemap_collection = get_sitemap_collection()
        for i, doc in enumerate(
            sitemap_collection.find({"iso": args.language_code, "content_type": None})
        ):
            update_utagdata(doc, i)
    elif args.update_iso and args.language and args.language_code:
        # Have to use dot notation or will overwrite the other fields in embedded doc
        sitemap_collection = get_sitemap_collection()
        sitemap_collection.update_many(
            {"language": args.language},
            {
                "$set": {
                    "iso": args.language_code,
                    "sitemap_prov.sitemap.iso": args.language_code,
                }
            },
        )
    elif args.update_lang_by_sitename and args.language_code and args.language:
        sitemap_collection = get_sitemap_collection()
        sitemap_collection.update_many(
            {"sitemap_prov.sitemap.site_name": args.update_lang_by_sitename},
            {
                "$set": {
                    "iso": args.language_code,
                    "language": args.language,
                    "sitemap_prov.sitemap.iso": args.language_code,
                    "sitemap_prov.sitemap.language": args.language,
                }
            },
        )
    elif args.update_metadata:
        if not args.ids_path:
            raise ValueError("Need ids path to update metadata")
        update_metadata(args.ids_path, n_workers=args.n_workers)
    elif args.dump_ids:
        dump_mongo_ids(args.dump_ids)
    elif args.rename_field and args.new_name:
        collection = get_sitemap_collection(args.port)
        collection.update_many({}, {"$rename": {args.rename_field: args.new_name}})
    elif args.remove_field:
        print(
            f"You're about to remove the field {args.remove_field} for all documents."
        )
        choice = input(f"Are you sure you want to? (y/n)\n")
        if choice.lower().startswith("y"):
            print(f"Removing field {args.remove_field}")
            collection = get_sitemap_collection(args.port)
            collection.update_many({}, {"$unset": {args.remove_field: ""}})
        else:
            print(f"Not removing field {args.remove_field}")
    elif args.add_latest:
        collection = get_sitemap_collection(args.port)
        collection.update_many({}, {"$set": {"latest": True}})
    else:
        print("No valid arguments selected")


def update_utagdata(doc, i):
    sitemap_collection = get_sitemap_collection()

    docid = doc["_id"]
    html = doc["original_html"]
    soup = BeautifulSoup(html, features="lxml")
    scripts = soup.find_all("script", {"type": "text/javascript"})
    utag_data = extract_utag_data(scripts, doc["url"])
    sitemap_collection.update_one(
        {"_id": docid},
        {
            "$set": {
                "utag_data": utag_data,
                "content_type": utag_data.get("content_type"),
            }
        },
    )
    print(i)


def extract_ld_json(scripts: Optional[Sequence[Tag]]) -> Dict:
    if scripts is None:
        # Has no scripts so return empty
        return {}
    for script in scripts:
        # text = script.getText()
        if script:
            if script.contents:
                json_text = script.contents[0]
                if json_text:
                    return json5.loads(json_text)
    # Couldn't find a match so return empty
    return {}


def process_doc(doc: Dict) -> Optional[UpdateOne]:
    docid = doc["_id"]
    html = doc.get("original_html", None)
    if not html:
        return None
    soup = BeautifulSoup(html, features="lxml")
    authors = soup.find_all("meta", {"name": "Author"})
    author_list = [author["content"] for author in authors]
    scripts = soup.find_all("script", {"type": "application/ld+json"})
    ld_json = extract_ld_json(scripts)
    date_published = ld_json.get("datePublished")
    date_modified = ld_json.get("dateModified")
    if date_published:
        date_published = datetime.fromisoformat(date_published)
    if date_modified:
        date_modified = datetime.strptime(date_modified, "%Y-%m-%d %H:%M:%SZ")
    return UpdateOne(
        {"_id": docid},
        {
            "$set": {
                "date_modified": date_modified,
                "date_published": date_published,
                "authors": author_list,
                "application_ld_json": ld_json,
            }
        },
    )


def _work(queue: JoinableQueue, worker_id: int) -> None:
    print(f"Starting worker {worker_id}")
    collection = get_sitemap_collection()
    write_operations = []
    while True:
        i, batch = queue.get()
        docs = collection.find({"_id": {"$in": batch}})
        for doc in docs:
            update_operation = process_doc(doc)
            if update_operation:
                write_operations.append(update_operation)
            if len(write_operations) >= 5:
                collection.bulk_write(write_operations)
                write_operations = []
        # Write the rest of operations
        if write_operations:
            collection.bulk_write(write_operations)
            write_operations = []
        queue.task_done()


def update_metadata(id_path: str, n_workers: int = 1, batch_size: int = 50):
    # Create a queue for batches of IDs
    queue: JoinableQueue = JoinableQueue()

    workers = [Process(target=_work, args=(queue, i)) for i in range(n_workers)]
    for worker in workers:
        worker.daemon = True
        worker.start()

    print(f"Loading IDs from {id_path}")
    article_count = 0
    batch_count = 0
    with open(id_path, encoding="utf8") as id_file:
        batch = []
        for line in id_file:
            batch.append(ObjectId(line.strip()))
            article_count += 1
            if len(batch) == batch_size:
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


def dump_mongo_ids(outfilepath: str) -> None:
    collection = get_sitemap_collection()
    with open(outfilepath, "w", encoding="utf8") as outfile:
        for doc in collection.find({}, {"_id": 1}):
            print(doc["_id"], file=outfile)


if __name__ == "__main__":
    update_fields()
