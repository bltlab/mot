"""
Script to do comparison of sitemaps from different dates.
"""
import os
from argparse import ArgumentParser
from os.path import basename
from typing import List, Dict

from jsonlines import jsonlines

from extraction.scraper import read_filemap, SitemapFile, pages_from_sitemaps


def check_new_languages(
    early_sitemap_by_lang: Dict[str, List[SitemapFile]],
    late_sitemap_by_lang: Dict[str, List[SitemapFile]],
):
    new_langs = set(late_sitemap_by_lang) - set(early_sitemap_by_lang)
    if new_langs:
        print(f"New languages: {new_langs}")
    else:
        print("No new languages")


def json_safe(page_dict: Dict) -> Dict:
    if page_dict["sitemap_last_modified"]:
        page_dict["sitemap_last_modified"] = str(
            page_dict["sitemap_last_modified"].isoformat()
        )
    return page_dict


def compare_sitemaps():
    parser = ArgumentParser()
    parser.add_argument("earlier_filemap")
    parser.add_argument("later_filemap")
    parser.add_argument("--early-sitemap-dir")
    parser.add_argument("--late-sitemap-dir")
    parser.add_argument("--outdir")
    args = parser.parse_args()

    early_sitemap_by_lang = read_filemap(args.earlier_filemap)
    late_sitemap_by_lang = read_filemap(args.later_filemap)

    os.makedirs(args.outdir, exist_ok=True)

    check_new_languages(early_sitemap_by_lang, late_sitemap_by_lang)
    with open(
        os.path.join(args.outdir, "lost_urls.txt"), "w", encoding="utf8"
    ) as outfile, open(
        os.path.join(args.outdir, f"new_counts{basename(args.later_filemap)}"),
        "w",
        encoding="utf8",
    ) as tsv_file:
        pages_to_dump = []
        for lang in early_sitemap_by_lang:
            # lang = "orm"
            early_sitemaps = early_sitemap_by_lang[lang]
            late_sitemaps = late_sitemap_by_lang[lang]

            diff_sitemap = set([s.filename for s in late_sitemaps]) - set(
                [s.filename for s in early_sitemaps]
            )
            print("Diff in sitemap names")
            print(diff_sitemap)

            early_pages_by_url = {
                p.url: p
                for p in pages_from_sitemaps(early_sitemaps, args.early_sitemap_dir)
            }
            late_pages_by_url = {
                p.url: p
                for p in pages_from_sitemaps(late_sitemaps, args.late_sitemap_dir)
            }
            added_later = set(late_pages_by_url) - set(early_pages_by_url)
            lost_urls = set(early_pages_by_url) - set(late_pages_by_url)
            print(f"{lang} added later: {len(added_later)}")
            print(f"{lang}\t{len(added_later)}", file=tsv_file)
            for added_url in added_later:
                pages_to_dump.append(late_pages_by_url[added_url])
            print(f"{lang} lost urls: {len(lost_urls)}", file=outfile)
            for url in lost_urls:
                print(url, file=outfile)
            print(file=outfile)

    # Dump the new sitemap pages to jsonl format instead of creating filemaps and sitemap dirs
    # TODO consider just making all of scraping use this and prepare sitemaps to list of pages
    # Don't need a utf8 encoding as the jsonlines open does this for us
    with jsonlines.open(
        os.path.join(args.outdir, f"new_urls-{basename(args.later_filemap)}"),
        "w",
    ) as writer:
        writer.write_all([json_safe(page.to_dict()) for page in pages_to_dump])


if __name__ == "__main__":
    compare_sitemaps()
