#! /usr/bin/env python
"""
Finds all the urls it can from the wayback machine / archive.org
See https://github.com/internetarchive/wayback/blob/master/wayback-cdx-server/README.md
"""
import os
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
from typing import DefaultDict, List, Generator
from urllib import request

from jsonlines import jsonlines

from extraction.downloadsitemaps import read_voa_domains, Sitemap, Domain
from extraction.scraper import Page, SitemapFile
from extraction.utils import ARCHIVEDOTORG
from scripts.comparesitemaps import json_safe
from http.client import IncompleteRead


def date_from_archive(time: int) -> datetime:
    """
    Archive.org has dates in this format: 20100527121226,
    where the date is 2010-05-27
    """
    time_str = str(time)
    return datetime(
        int(time_str[:4]),
        int(time_str[4:6]),
        int(time_str[6:8]),
        int(time_str[8:10]),
        int(time_str[10:12]),
    )


def yield_pages_from_archive(
    domain: Domain, non_useful_outdir: str
) -> Generator[Page, None, None]:
    """
    Generator to query archive and get urls
    """
    query_string = (
        f"https://web.archive.org/cdx/search/cdx?url={domain.news}*&output=txt"
    )
    # Store urls and all the snapshots (timestamps)
    # When grabbing the url, use most recent snapshot for the url
    url_times: DefaultDict[str, List[int]] = defaultdict(list)
    with open(
        os.path.join(non_useful_outdir, domain_to_outpath(domain)), "w", encoding="utf8"
    ) as non_useful_outfile_log:
        try:
            weburl = request.urlopen(query_string)
            try:
                for i, line in enumerate(weburl):
                    if i % 100 == 0:
                        print(i, " completed")
                    fields = line.split()
                    time = int(fields[1])

                    try:
                        url_key = fields[0].decode("utf8")
                        url = fields[2].decode("utf8")

                    except Exception as e:
                        print("Failed to get page")
                        print(e)
                        print("Continuing...")
                        continue
                    # Google tag manager urls don't appear to be actual webpages
                    # Also other javascript and other things we probably don't want to scrape
                    # write them to a file to be able to review if needed
                    if not url_key.endswith(".html"):
                        print(url, file=non_useful_outfile_log)
                        continue
                    else:
                        url = url.replace(":80", "")
                        url_times[url].append(time)

                for url in url_times:
                    # print(url, max(url_times[url]))
                    archive_datetime = max(url_times[url])
                    wayback_url = (
                        f"https://web.archive.org/web/{archive_datetime}/{url}"
                    )

                    yield Page(
                        SitemapFile(
                            ARCHIVEDOTORG,
                            Sitemap(
                                url=domain.news,
                                iso=domain.iso,
                                language=domain.language,
                                site_name=domain.site_name,
                                timestamp=date_from_archive(
                                    archive_datetime
                                ).isoformat(),
                                region=domain.region,
                            ),
                        ),
                        url,
                        sitemap_last_modified=None,
                        changefreq=None,
                        priority=None,
                        archive_url=wayback_url,
                    )
            except IncompleteRead as e:
                # Oh well, keep going
                print(f"error: {e}")
                print(query_string)
                print(weburl)
        # Bare exception, bad practice but not sure what exceptions might show up
        except Exception as e:
            print(e, query_string)


def download_wayback_urls():
    parser = ArgumentParser()
    parser.add_argument("domain_tsv", help="domains tsv")
    parser.add_argument("outdir")

    args = parser.parse_args()
    useful_outdir = os.path.join(args.outdir, "useful")
    non_useful_outdir = os.path.join(args.outdir, "non-useful")
    os.makedirs(useful_outdir, exist_ok=True)
    os.makedirs(non_useful_outdir, exist_ok=True)
    for domain in read_voa_domains(args.domain_tsv):

        pages_to_dump = yield_pages_from_archive(domain, non_useful_outdir)

        outpath = domain_to_outpath(domain)
        with jsonlines.open(
            os.path.join(useful_outdir, f"urls-{outpath}"),
            "w",
        ) as writer:
            writer.write_all([json_safe(page.to_dict()) for page in pages_to_dump])


def domain_to_outpath(domain: Domain) -> str:
    outpath = (
        domain.news.replace("http://", "")
        .replace("www.", "")
        .replace("www1.", "")
        .replace("/", "_")
        .rstrip("_")
    ) + ".json"
    return outpath


if __name__ == "__main__":
    download_wayback_urls()
