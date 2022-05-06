#! /usr/bin/env python
"""
Script to download sitemaps and check if they have obvious text in them.
Reports success and failures and number of docs that have text
"""
import asyncio
import json
import os
import re

from asyncio import Semaphore
from collections import defaultdict, Counter
from datetime import datetime

from typing import Sequence, Dict, Generator, Iterable, List, Optional, Tuple

import aiohttp
import click
import json5
from aiohttp import ClientSession, ClientResponseError
from attr import attrs, attrib
from bs4 import BeautifulSoup, Tag
from lxml import etree, objectify
from lxml.etree import XMLSyntaxError
from pymongo import MongoClient, DESCENDING, HASHED
from pymongo.collection import Collection

from extraction.downloadsitemaps import Sitemap
from extraction.utils import get_sitemap_collection, get_publication_date_from_utag

VOA_CORPUS = "voa_corpus"
VAR_UTAG_PATTERN = re.compile(r"var\s+utag_data\s*=\s*({.*})")


@attrs(frozen=True, auto_attribs=True)
class SitemapFile:
    filename: str
    sitemap: Sitemap

    @classmethod
    def from_fields(cls, fields: Sequence[str]) -> "SitemapFile":
        return SitemapFile(fields[0], Sitemap(*fields[1:]))

    def to_dict(self):
        ret = {}
        ret["filename"] = self.filename
        ret["sitemap"] = self.sitemap.to_dict()
        return ret


@attrs(frozen=True, auto_attribs=True)
class Page:
    sitemap_prov: SitemapFile
    url: Optional[str]
    sitemap_last_modified: Optional[datetime]
    changefreq: Optional[str]
    priority: Optional[str]
    metadata: Dict = attrib(factory=dict)
    archive_url: Optional[str] = attrib(default=None)

    @classmethod
    def from_node(cls, node, sitemap: SitemapFile) -> "Page":
        timestamp: Optional[datetime] = None
        url: Optional[str] = None
        changefreq: Optional[str] = None
        priority: Optional[str] = None
        video = {}
        news = {}
        metadata = {}
        for tag in node:
            if etree.QName(tag).localname == "loc":
                url = tag.text
            elif etree.QName(tag).localname == "lastmod":
                timestamp = cls.parse_timestamp(tag.text)
            elif etree.QName(tag).localname == "priority":
                priority = tag.text
            elif etree.QName(tag).localname == "changefreq":
                changefreq = tag.text
            elif etree.QName(tag).localname == "news":
                news = gather_subtags(tag)
            elif etree.QName(tag).localname == "video":
                video = gather_subtags(tag)
            else:
                print("Name not handled:")
                print(etree.QName(tag).localname)
                print()
            if news:
                metadata["news"] = news
            if video:
                metadata["video"] = video
        return Page(sitemap, url, timestamp, changefreq, priority, metadata)

    @classmethod
    def parse_timestamp(cls, s: Optional[str]) -> Optional[datetime]:
        """
        Fix timestamps of format 2021-06-08T16:03:48.75Z
        Copied from fix timestamps code, not yet tested.
        """
        # In the case that the sitemaps didn't have a timestamp just pass along what they had
        if not s or s == "None":
            return None
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

    def to_dict(self):
        ret = {}
        ret.update(self.metadata)
        ret["url"] = self.url
        ret["sitemap_last_modified"] = self.sitemap_last_modified
        ret["changefreq"] = self.changefreq
        ret["priority"] = self.priority
        ret["sitemap_prov"] = self.sitemap_prov.to_dict()
        ret["archive_url"] = self.archive_url
        return ret

    @classmethod
    def from_json(cls, page_json: Dict):
        sitemap_prov = page_json["sitemap_prov"]
        sitemap = SitemapFile(
            sitemap_prov["filename"], Sitemap.from_json(sitemap_prov["sitemap"])
        )
        url = page_json["url"]
        # Handle old version with timestamp or new version with sitemap_last_modified
        if "timestamp" in page_json:
            timestamp = cls.parse_timestamp(page_json["timestamp"])
        else:
            timestamp = cls.parse_timestamp(page_json["sitemap_last_modified"])
        changefreq = page_json["changefreq"]
        priority = page_json["priority"]
        metadata = {}
        if page_json.get("news"):
            metadata["news"] = page_json.get("news")
        if page_json.get("video"):
            metadata["video"] = page_json.get("video")
        archive_url = page_json.get("archive_url")

        return Page(
            sitemap,
            url,
            timestamp,
            changefreq,
            priority,
            metadata,
            archive_url=archive_url,
        )


def read_filemap(filemap: str) -> Dict[str, List[SitemapFile]]:
    sitemaps = defaultdict(list)
    with open(filemap, "r", encoding="utf8") as f:
        for i, line in enumerate(f):
            # Skip header
            if i == 0:
                continue
            # make a sitemaps by language, store each sitemap with it's prov info
            fields = line.split("\t")
            sitemaps[fields[2]].append(
                SitemapFile.from_fields(
                    # filename, url, iso, language, site_name, timestamp, region
                    (
                        fields[0],
                        fields[1],
                        fields[2],
                        fields[3],
                        fields[4],
                        fields[5],
                        fields[6],
                    )
                )
            )
    return sitemaps


def gather_subtags(tag):
    """There's a bunch of subtags on video and news, so just throw those in a dict for now"""
    return {etree.QName(subtag).localname: subtag.text for subtag in tag}


def pages_from_sitemaps(
    sitemap_list: Sequence[SitemapFile], sitemap_dir: str
) -> Generator[Page, None, None]:
    existing_urls = set()
    for sitemap in sitemap_list:
        with open(os.path.join(sitemap_dir, sitemap.filename), "rb") as site_file:
            try:
                tree = etree.XML(site_file.read())
            except XMLSyntaxError as e:
                print(f"Couldn't parse {sitemap.filename}. {e}")
                continue
            objectify.deannotate(tree, cleanup_namespaces=True)
            for node in tree:
                page = Page.from_node(node, sitemap)
                if not page.url:
                    continue
                if page.url in existing_urls:
                    continue
                existing_urls.add(page.url)
                yield page


def is_valid(text: str) -> bool:
    """
    Simple check to eliminate obviously bad text in paragraph tags.
    """
    text = text.strip()
    if not text:
        return False
    elif text.startswith("No media source currently available"):
        return False
    elif text.startswith("Already have an account?"):
        return False
    elif text.startswith("Log in"):
        return False
    elif text.startswith("Sign up"):
        return False
    elif text.startswith("Not a registered user?"):
        return False
    else:
        return True


@attrs(frozen=True, auto_attribs=True)
class PageResult:
    success: bool
    time_retrieved: datetime
    content: Optional[str]
    error_message: Optional[str]


async def request_page(url: Optional[str], session: ClientSession) -> PageResult:
    success = False
    error_message = None
    timestamp = datetime.now()
    # timestamp = datetimenow.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if not url:
        return PageResult(success, timestamp, None, "Missing url")
    try:
        resp = await session.request(method="GET", url=url)
        resp.raise_for_status()
        print(f"Got response [{resp.status}] for URL: {url}")
        success = True
        content = await resp.text()
        return PageResult(success, timestamp, content, error_message)
    except ClientResponseError as e:
        print(e)
        return PageResult(success, timestamp, None, str(e))
    except Exception as e:
        print(e)
        return PageResult(success, timestamp, None, str(e))


def extract_utag_data(scripts: Optional[Sequence[Tag]], url: Optional[str]) -> Dict:
    if scripts is None:
        # Has no scripts so return empty
        return {}
    try:
        for script in scripts:
            # text = script.getText()
            if script:
                match = VAR_UTAG_PATTERN.search(str(script))
                if match:
                    return json5.loads(match.group(1))
    except ValueError as e:
        with open("utag_data.log", "a", encoding="utf8") as logfile:
            print(url, file=logfile)
            print(e, file=logfile)
            print()
        return {}
    # Couldn't find a match so return empty
    return {}


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


async def scrape_page(
    page: Page, session: ClientSession, mongo_collection: Collection, sem: Semaphore
):
    # # Bare except is bad, but not clear what error is thrown
    # failures += 1
    async with sem:
        page_result = await request_page(page.url, session)
    has_ptags = False
    html_tag_metadata = {}
    # text = ""
    if page_result.content is not None:
        soup = BeautifulSoup(page_result.content, features="lxml")
        paragraphs = soup.find_all("p")
        if paragraphs:
            texts = [p.getText() for p in paragraphs if is_valid(p.getText())]
            if texts:
                # text = "\n".join(texts)
                has_ptags = True
        title = soup.find("meta", {"name": "title"})
        description = soup.find("meta", {"name": "description"})
        canonical_link = soup.find("link", {"rel": "canonical"})
        keywords = soup.find("meta", {"name": "keywords"})
        authors = soup.find_all("meta", {"name": "Author"})
        author_list = [author["content"] for author in authors]
        title = await get_content(title)
        description = await get_content(description)
        canonical_link = await get_content(canonical_link, "href")
        keywords = await get_content(keywords)
        if keywords:
            keywords = [keyword.strip() for keyword in keywords.split(",")]
        else:
            keywords = []

        scripts = soup.find_all("script", {"type": "text/javascript"})
        utag_data = extract_utag_data(scripts, page.url)
        ld_scripts = soup.find_all("script", {"type": "application/ld+json"})
        ld_json = extract_ld_json(ld_scripts)
        date_published = ld_json.get("datePublished")
        date_modified = ld_json.get("dateModified")
        if date_published:
            date_published = datetime.fromisoformat(date_published)
        if not date_published:
            date_published = get_publication_date_from_utag(utag_data)
        if date_modified:
            date_modified = datetime.strptime(date_modified, "%Y-%m-%d %H:%M:%SZ")

        html_tag_metadata = {
            "title": title,
            "description": description,
            "keywords": keywords,
            "canonical_link": canonical_link,
            "utag_data": utag_data,
            "content_type": utag_data.get("content_type"),
            "has_ptags": has_ptags,
            "date_published": date_published,
            "date_modified": date_modified,
            "application_ld_json": ld_json,
            "authors": author_list,
        }

    await insert_page(
        mongo_collection, page_result, html_tag_metadata=html_tag_metadata, page=page
    )


async def get_content(tag, att_name: str = "content") -> Optional[str]:
    if tag:
        tag = tag.get(att_name, None)
    return tag


async def insert_page(
    collection: Collection,
    page_result: PageResult,
    *,
    html_tag_metadata: Dict,
    page: Page,
):
    document = page.to_dict()
    document.update(html_tag_metadata)
    document["original_html"] = str(page_result.content)
    document["success"] = page_result.success
    document["language"] = page.sitemap_prov.sitemap.language
    document["iso"] = page.sitemap_prov.sitemap.iso
    document["time_retrieved"] = page_result.time_retrieved
    document["error_message"] = page_result.error_message
    if page_result.success and document.get("canonical_link"):
        existing_docids = [
            doc["_id"]
            for doc in collection.find({"canonical_link": document["canonical_link"]})
        ]
        existing_docids.extend(
            [doc["_id"] for doc in collection.find({"url": document["url"]})]
        )
        existing_docids = list(set(existing_docids))
        if existing_docids and document["success"]:
            # Collision of canonical links, check and set latest if not an error
            document["latest"] = True
            try:
                collection.insert_one(document)
                collection.update_many(
                    {"_id": {"$in": existing_docids}}, {"$set": {"latest": False}}
                )
            except UnicodeEncodeError as e:
                print(f"Unicode error on {page.url}")
                document = page.to_dict()
                # Set latest flag to false so we can still grab latest error-free version of
                #   canonical link
                document["latest"] = False
                document.update(
                    {
                        "error_message": str(e),
                        "success": False,
                        "iso": page.sitemap_prov.sitemap.iso,
                        "language": page.sitemap_prov.sitemap.language,
                        "time_retrieved": page_result.time_retrieved,
                    }
                )
                collection.insert_one(document)
        else:
            await insert_without_deduplication(collection, document, page, page_result)
    else:
        await insert_without_deduplication(collection, document, page, page_result)


async def insert_without_deduplication(collection, document, page, page_result):
    # Insert document normally
    try:
        document["latest"] = True
        collection.insert_one(document)
    except UnicodeEncodeError as e:
        print(f"Unicode error on {page.url}")
        document = page.to_dict()
        document["latest"] = False
        document.update(
            {
                "error_message": str(e),
                "success": False,
                "iso": page.sitemap_prov.sitemap.iso,
                "language": page.sitemap_prov.sitemap.language,
                "time_retrieved": page_result.time_retrieved,
            }
        )
        collection.insert_one(document)


async def scrape_and_insert(
    sitemap_collection: Collection,
    pages_generator: Iterable[Page],
    num_connections: int = 8,
):
    sem = asyncio.Semaphore(num_connections)
    connector = aiohttp.TCPConnector(limit_per_host=50)
    async with ClientSession(connector=connector) as session:
        tasks = []
        for page in pages_generator:
            tasks.append(
                asyncio.ensure_future(
                    scrape_page(
                        page,
                        session=session,
                        mongo_collection=sitemap_collection,
                        sem=sem,
                    )
                )
            )
            # await asyncio.sleep(1)
        await asyncio.gather(*tasks)


@click.group()
def scraper_cli():
    pass


@scraper_cli.command()
@click.argument("filemap")
@click.argument("sitemap_dir")
@click.option("--languages", "-l", multiple=True)
@click.option("--exclude-languages", "-e", multiple=True)
@click.option("--doc-limit", type=int)
@click.option("--drop-database", type=bool, default=False)
@click.option("--port", default=27200, type=int)
@click.option("--num-connections", default=8, type=int)
def scrape(
    filemap: str,
    sitemap_dir: str,
    languages: Tuple[str, ...],
    exclude_languages: Tuple[str, ...],
    doc_limit: Optional[int],
    drop_database: bool,
    port: int = 27200,
    num_connections: int = 8,
):
    client = MongoClient(port=port)
    if drop_database:
        choice = input(
            "You're about to drop the entire database before scraping. Are you sure you want to? (y/n)"
        )
        if choice.lower().startswith("y"):
            print("Dropping database...")
            client.drop_database(VOA_CORPUS)
    voa_corpus = client.voa_corpus
    sitemap_collection: Collection = voa_corpus.sitemaps

    sitemaps_by_language = read_filemap(filemap)
    for lang in sitemaps_by_language:
        if exclude_languages and lang in set(exclude_languages):
            print(f"Skipping {lang}")
            continue
        if not languages or lang in set(languages):
            pages_generator = pages_from_sitemaps(
                sitemaps_by_language[lang], sitemap_dir
            )
            if doc_limit:
                pages_generator = (p for p in list(pages_generator)[:doc_limit])
            asyncio.run(
                scrape_and_insert(sitemap_collection, pages_generator, num_connections)
            )
    create_scrape_indices(sitemap_collection)


def create_scrape_indices(sitemap_collection):
    print("Creating indices")
    sitemap_collection.create_index(
        [("has_text", DESCENDING), ("success", DESCENDING), ("language", DESCENDING)]
    )
    sitemap_collection.create_index([("language", DESCENDING)])
    sitemap_collection.create_index([("iso", DESCENDING)])
    sitemap_collection.create_index(
        [("language", DESCENDING), ("content_type", DESCENDING)]
    )
    sitemap_collection.create_index([("canonical_link", HASHED)])
    sitemap_collection.create_index([("url", DESCENDING)])


def read_pages_from_json(sitemap_diff_json_path: str) -> Generator[Page, None, None]:
    with open(sitemap_diff_json_path, "r", encoding="utf8") as infile:
        for line in infile:
            page_json = json.loads(line)
            yield Page.from_json(page_json)


@scraper_cli.command()
@click.argument("sitemap_diff_json_path")
@click.option("--port", default=27200, type=int)
@click.option("--num-connections", default=8, type=int)
def update(sitemap_diff_json_path: str, port: int = 27200, num_connections: int = 8):
    sitemap_collection = get_sitemap_collection(port)
    pages_generator = read_pages_from_json(sitemap_diff_json_path)
    asyncio.run(scrape_and_insert(sitemap_collection, pages_generator, num_connections))
    create_scrape_indices(sitemap_collection)


@scraper_cli.command()
@click.argument("archive_pages_dir")
@click.option("--port", default=27200, type=int)
@click.option("--num-connections", default=8, type=int)
def scrape_waybacks(
    archive_pages_dir: str, port: int = 27200, num_connections: int = 8
):
    """
    This is essentially the same as update for now, but is kept separate
    in case we wish to do anything more complicated with archive.org and the
    wayback machine later
    """
    sitemap_collection = get_sitemap_collection(port)
    for filepath in os.listdir(archive_pages_dir):
        pages_generator = read_pages_from_json(
            os.path.join(archive_pages_dir, filepath)
        )
        asyncio.run(
            scrape_and_insert(sitemap_collection, pages_generator, num_connections)
        )
    create_scrape_indices(sitemap_collection)


if __name__ == "__main__":
    scraper_cli()
