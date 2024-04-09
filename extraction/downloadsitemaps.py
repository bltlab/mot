#! /usr/bin/env python
"""
Script to go through all the VOA sites and get sitemaps and extract
from them additional sitemaps.
"""
import gzip
import os
import re
import shutil
from argparse import ArgumentParser
from os.path import basename
from typing import List, Optional, Generator, Iterable, Dict
from urllib import request
import wget
from attr import attrs
from lxml import etree, objectify


@attrs(frozen=True, auto_attribs=True)
class Domain:
    iso: str
    language: str
    site_name: str
    news: str
    radio: Optional[str]
    video: Optional[str]
    tv: Optional[str]
    direct: Optional[str]
    region: Optional[str]

    def categories(self):
        """Get all the links for categories like news, radio, etc that exist for a domain."""
        cats = [self.news, self.radio, self.video, self.tv, self.direct]
        return [cat for cat in cats if cat]


@attrs(frozen=True, auto_attribs=True)
class Sitemap:
    url: str
    iso: str
    language: str
    site_name: str
    timestamp: Optional[str]
    region: Optional[str]

    def to_fields(self):
        return "\t".join(
            [
                self.url,
                self.iso,
                self.language,
                self.site_name,
                str(self.timestamp),
                str(self.region),
            ]
        )

    def to_dict(self):
        return {
            "url": self.url,
            "iso": self.iso,
            "language": self.language,
            "site_name": self.site_name,
            "timestamp": self.timestamp,
            "region": self.region,
        }

    @classmethod
    def from_json(cls, sitemap_json: Dict):
        return cls(
            sitemap_json["url"],
            sitemap_json["iso"],
            sitemap_json["language"],
            sitemap_json["site_name"],
            sitemap_json["timestamp"],
            sitemap_json["region"],
        )


def clean_fields(fields: List[str]):
    return [field.strip() if field.strip() else None for field in fields]


def read_voa_domains(domain_tsv_path: str) -> List[Domain]:
    """
    Parse the VOA domain tsv file to get relevant info.
    This currently assumes regions are specified in the line above all the entries.
    """
    domains = []
    with open(domain_tsv_path, "r", encoding="utf8") as domain_file:
        region = None
        for i, line in enumerate(domain_file):
            if i == 0:
                # Skip the header
                continue
            fields = line.split("\t")
            fields = clean_fields(fields)
            if fields[0]:
                region = fields[0]
            if fields[1]:
                domains.append(
                    Domain(
                        fields[1],
                        fields[2],
                        fields[3],
                        fields[4],
                        fields[5],
                        fields[6],
                        fields[7],
                        fields[8],
                        region,
                    )
                )
    return domains


def find_sitemaps(domains: List[Domain]) -> Generator[Sitemap, None, None]:
    """Get the sitemaps from the web domains by appending /sitemap.xml"""
    for domain in domains:
        # Only use news domain as it seems radio/tv etc sitemaps have errors, for now
        category = domain.news
        if category:
            appended_url = category + "/sitemap.xml"
            try:
                weburl = request.urlopen(appended_url)
            # Bare exception, bad practice but not sure what exceptions might show up
            except Exception as e:
                print(e, appended_url)

            weburl_content = weburl.read()
            if not weburl or not weburl_content:
                print(f"Warning appended_url: {appended_url} has no content to parse")
                continue
            sitemap_tree = etree.XML(weburl_content)
            objectify.deannotate(sitemap_tree, cleanup_namespaces=True)
            # Find the url and the timestamp
            for sitemap in sitemap_tree:
                timestamp = None
                sitemap_url = None
                for tag in sitemap:
                    if tag.text.endswith("Hreflang"):
                        # Appears to not contain anything
                        continue
                    if etree.QName(tag).localname == "loc":
                        sitemap_url = tag.text
                    elif etree.QName(tag).localname == "lastmod":
                        timestamp = tag.text
                if not sitemap_url:
                    continue

                yield Sitemap(
                    sitemap_url,
                    domain.iso,
                    domain.language,
                    domain.site_name,
                    timestamp,
                    domain.region,
                )


def gunzip(filepath: str) -> str:
    """
    Uncompress a gzip file.
    """
    if filepath.endswith(".gz"):
        with gzip.open(filepath, "rb") as f_in:
            with open(filepath.rstrip(".gz"), "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(filepath)
        return filepath.rstrip(".gz")
    else:
        raise ValueError(
            "This file shouldn't be gunzipped since the extension isn't .gz"
        )


def sanitize_url(url: str) -> str:
    """Remove parts of url string we don't want or can't use as a filename"""
    base = (
        url.replace("?", "_")
        .replace(",", "_")
        .replace("=", "_")
        .replace("https://www.", "")
        .replace("http://www.", "")
        .replace("https://", "")
        .replace("/", "_")
    )
    return re.sub(r"\s+", "_", base)


def dump_sitemaps(filemap_path: str, sitemaps: Iterable[Sitemap], outdir: str) -> None:
    """
    Download and write sitemaps to the outdir. Also create a filemap with the filename and
    other info about the particular sitemap like language, iso code, url it came from,
    timestamp etc.
    """
    filename_set = set()
    with open(filemap_path, "w", encoding="utf8") as filemap_out:
        # Write header
        print(
            "\t".join(
                [
                    "Filename",
                    "Url",
                    "ISO",
                    "Language",
                    "Sitename",
                    "Timestamp",
                    "Region",
                ]
            ),
            file=filemap_out,
        )
        for sitemap in sitemaps:
            # This is a bit of a messy way to turn the urls into filenames, but works for now
            filename = sanitize_url(sitemap.url)
            if filename in filename_set:
                raise ValueError(f"filename {filename} already exists")
            filename_set.add(filename)
            path = os.path.join(outdir, filename)
            wget.download(sitemap.url, path)
            if sitemap.url.endswith(".gz"):
                filename = gunzip(path)
            print(f"{basename(filename)}\t{sitemap.to_fields()}", file=filemap_out)


def download_and_extract():
    parser = ArgumentParser()
    parser.add_argument("domain_tsv")
    parser.add_argument("outdir")
    parser.add_argument(
        "filemap",
        help="Path to write a tsv of the filenames and their provenance, and other info",
    )
    args = parser.parse_args()

    domains = read_voa_domains(args.domain_tsv)

    sitemaps = find_sitemaps(domains)

    os.makedirs(args.outdir, exist_ok=True)
    dump_sitemaps(args.filemap, sitemaps, args.outdir)


if __name__ == "__main__":
    download_and_extract()
