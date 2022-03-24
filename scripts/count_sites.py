#! /usr/bin/env python
"""
Script to get counts of all the sites from all the sitemaps by language and with duplicates by
language and by sitemap.
"""
import csv
import os
from argparse import ArgumentParser

from collections import Counter
from typing import Sequence, Tuple

from lxml import etree, objectify
from lxml.etree import XMLSyntaxError

from extraction.downloadsitemaps import sanitize_url
from extraction.scraper import Page, read_filemap, SitemapFile


def gather_subtags(tag):
    """There's a bunch of subtags on video and news, so just throw those in a dict for now"""
    return {etree.QName(subtag).localname: subtag.text for subtag in tag}


def count_pages(
    sitemap_list: Sequence[SitemapFile], sitemap_dir: str, lang_dir: str
) -> Tuple[int, int, Counter, Counter]:
    duplicates = 0
    duplicates_by_sitemap: Counter = Counter()
    count_by_sitemap: Counter = Counter()
    url_lines = 0
    existing_urls = set()
    header = [
        "Url",
        "Filename",
        "Timestamp",
        "Sitemap",
        "Change.Freq",
        "Priority",
        "Other.Data",
    ]
    with open(os.path.join(lang_dir, "urls.tsv"), "w", encoding="utf8") as tsv_file:
        writer = csv.DictWriter(tsv_file, fieldnames=header, dialect="excel-tab")
        writer.writeheader()
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
                        duplicates += 1
                        duplicates_by_sitemap[sitemap] += 1
                        continue
                    existing_urls.add(page.url)
                    fields = [
                        page.url,
                        sanitize_url(page.url),
                        page.sitemap_last_modified,
                        sitemap.filename,
                        page.changefreq,
                        page.priority,
                    ]
                    for label, value in page.metadata.items():
                        fields.append(f"{label}:{value}")

                    writer.writerow(
                        {
                            "Url": page.url,
                            "Filename": sanitize_url(page.url),
                            "Timestamp": page.sitemap_last_modified,
                            "Sitemap": sitemap.filename,
                            "Change.Freq": page.changefreq,
                            "Priority": page.priority,
                            "Other.Data": page.metadata,
                        }
                    )
                    url_lines += 1
                    count_by_sitemap[sitemap] += 1
    return url_lines, duplicates, count_by_sitemap, duplicates_by_sitemap


def scrape():
    parser = ArgumentParser()
    parser.add_argument("filemap")
    parser.add_argument("sitemap_dir")
    parser.add_argument("--languages", nargs="+")
    parser.add_argument("outdir")
    args = parser.parse_args()

    sitemaps_by_language = read_filemap(args.filemap)
    counts_by_lang = {}
    counts_by_sitemap = Counter()
    duplicates_by_sitemap = Counter()
    for lang in sitemaps_by_language:
        if not args.languages or lang in set(args.languages):
            lang_dir = os.path.join(args.outdir, lang)
            os.makedirs(lang_dir, exist_ok=True)
            urls, duplicates, lang_sitemap_counts, lang_duplicate_sitemap = count_pages(
                sitemaps_by_language[lang], args.sitemap_dir, lang_dir
            )
            counts_by_sitemap.update(lang_sitemap_counts)
            duplicates_by_sitemap.update(lang_duplicate_sitemap)
            counts_by_lang[lang] = (urls, duplicates)
    with open(
        os.path.join(args.outdir, "url_counts_by_language.tsv"), "w", encoding="utf8"
    ) as counts_file:
        writer = csv.DictWriter(
            counts_file,
            fieldnames=["lang", "url.count", "duplicates.count"],
            dialect="excel-tab",
        )
        writer.writeheader()
        for lang in counts_by_lang:
            writer.writerow(
                {
                    "lang": lang,
                    "url.count": counts_by_lang[lang][0],
                    "duplicates.count": counts_by_lang[lang][1],
                }
            )

    with open(
        os.path.join(args.outdir, "url_counts_by_sitemap.tsv"), "w", encoding="utf8"
    ) as sitemap_counts_file:
        writer = csv.DictWriter(
            sitemap_counts_file,
            fieldnames=["sitemap", "lang", "url.count", "duplicates"],
            dialect="excel-tab",
        )
        writer.writeheader()
        for sitemap in sorted(set(counts_by_sitemap).union(duplicates_by_sitemap)):
            writer.writerow(
                {
                    "sitemap": sitemap.filename,
                    "lang": sitemap.sitemap.iso,
                    "url.count": counts_by_sitemap[sitemap],
                    "duplicates": duplicates_by_sitemap[sitemap],
                }
            )


if __name__ == "__main__":
    scrape()
