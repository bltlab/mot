#! /usr/bin/env python
"""
Script to extract text from json dumped scrapes from scrapes mongodb.
"""
import os
import json
import urllib.parse

import click
from torch import multiprocessing
from torch.multiprocessing import JoinableQueue, Process
from typing import (
    Generator,
    NamedTuple,
    List,
    Dict,
    Sequence,
    Any,
    Optional,
    Tuple,
    Union,
)
from bs4 import BeautifulSoup
import cld3
import pycountry
from spacy.tokenizer import Tokenizer

from extraction.dump_documents import (
    enqueue_json_docs,
    languages_from_filemap,
    create_date_query,
)
from extraction.segmentation import Segmenter, setup_segmenter, SEGMENTABLE_LANGUAGES
from extraction.tokenization import setup_tokenizer, TOKENIZABLE_LANGUAGES

# Thresholds for counting cld3 prediction as valid
CLD3_PROBABILITY_THRESHOLD = 0.9
CLD3_PROPORTION_THRESHOLD = 0.05


@click.group()
def cli():
    pass


class Document(NamedTuple):
    filename: str
    url: str
    url_origin: str
    content_type: str
    site_language: str
    time_published: Optional[str]
    time_modified: Optional[str]
    time_retrieved: Optional[str]
    title: str
    authors: Optional[List[str]]
    paragraphs: List[str]
    n_paragraphs: int
    n_chars: int
    parallel_english_article: Optional[str]
    cld3_detected_languages: Dict[str, Dict[str, Union[float, str]]]
    predicted_language: str
    sentences: Optional[List[List[str]]]
    tokens: Optional[List[List[List[str]]]]
    n_tokens: Optional[int]
    n_sentences: Optional[int]
    keywords: List[str]
    section: Optional[str]

    def to_dict(self):
        dictionary = self._asdict()
        if not dictionary["parallel_english_article"]:
            del dictionary["parallel_english_article"]
        if not dictionary["sentences"]:
            del dictionary["sentences"]
        if not dictionary["tokens"]:
            del dictionary["tokens"]
        return dictionary

    def update_filename(self, new_filename: str) -> "Document":
        """
        Updates the filename on an existing document.
        """
        # If we ever need to update more, consider using builder instead of this method
        return Document(
            filename=new_filename,
            url=self.url,
            url_origin=self.url_origin,
            content_type=self.content_type,
            site_language=self.site_language,
            time_published=self.time_published,
            time_modified=self.time_modified,
            time_retrieved=self.time_retrieved,
            title=self.title,
            authors=self.authors,
            paragraphs=self.paragraphs,
            n_paragraphs=self.n_paragraphs,
            n_chars=self.n_chars,
            parallel_english_article=self.parallel_english_article,
            cld3_detected_languages=self.cld3_detected_languages,
            predicted_language=self.predicted_language,
            sentences=self.sentences,
            tokens=self.tokens,
            n_tokens=self.n_tokens,
            n_sentences=self.n_sentences,
            keywords=self.keywords,
            section=self.section,
        )

    @classmethod
    def from_dict(cls, d: Dict[Any, Any]) -> "Document":
        """
        Convert json dictionary into a Document object
        """
        return Document(
            filename=d["filename"],
            url=d["url"],
            url_origin=d["url_origin"],
            content_type=d["content_type"],
            site_language=d["site_language"],
            time_published=d["time_published"],
            time_modified=d["time_modified"],
            time_retrieved=d["time_retrieved"],
            title=d["title"],
            authors=d.get("authors", []),
            paragraphs=d.get("paragraphs", []),
            n_paragraphs=d["n_paragraphs"],
            n_chars=d["n_chars"],
            parallel_english_article=d.get("parallel_english_article", None),
            cld3_detected_languages=d["cld3_detected_languages"],
            predicted_language=d["predicted_language"],
            sentences=d.get("sentences", []),
            tokens=d.get("tokens", []),
            n_tokens=d.get("n_tokens"),
            n_sentences=d.get("n_sentences"),
            keywords=d.get("keywords", []),
            section=d.get("section", None),
        )


def find_docpaths(inputdir: str) -> Generator[str, None, None]:
    """
    Walk the input dir and get all the files.
    Since each document contains language information, we don't need to maintain the
    directory structure.
    """
    for root, dirs, files in os.walk(inputdir):
        for file in files:
            if file.endswith(".json"):
                yield os.path.join(root, file)


def filter_confident_langs(
    cld3_predictions: Dict[str, Dict[str, Any]],
    probability_threshold: float = 0.9,
    proportion_threshold: float = 0.01,
) -> Dict[str, Dict[str, Any]]:
    """
    Filter cld3 predcitions to only those with probability and proportion
    above a certain threshold
    """
    return {
        lang: pred
        for lang, pred in cld3_predictions.items()
        if pred["probability"] >= probability_threshold
        and pred["proportion"] >= proportion_threshold
    }


def confident_multiple_languages(
    cld3_predictions: Dict[str, Dict[str, Any]],
) -> bool:
    if len(cld3_predictions) == 1:
        return False
    else:
        return (
            len(
                filter_confident_langs(
                    cld3_predictions,
                    probability_threshold=CLD3_PROBABILITY_THRESHOLD,
                    proportion_threshold=CLD3_PROPORTION_THRESHOLD,
                )
            )
            > 1
        )


def predict_language(iso: str, cld3_predictions: Dict[str, Dict[str, Any]]) -> str:
    """
    Returns ISO-639-3 language code given the 3 letter iso code from the sitemap
    and cld3 predictions.
    When cld3 is confident in one language, uses original sitemap language
    since cld3 doesn't predict all languages we encounter.
    If cld3 has high enough confidence and high enough proportion of the text
    for more than one language, "mul" is assigned.
    """
    if not cld3_predictions:
        return iso

    elif (
        iso != "eng"
        and len(cld3_predictions) == 1
        and list(cld3_predictions)[0] == "eng"
    ):
        return "eng"

    elif confident_multiple_languages(cld3_predictions):
        return "mul"
    else:
        return iso


def reasonable_len(
    tokens: Optional[Sequence[Sequence[Sequence[str]]]],
    n_chars: int,
    tok_len: int = 8,
    char_len: int = 20,
) -> bool:
    """
    Checks that tokens / sentences are reasonable length.
    Unreasonable is when there's no sentences or there's only one sentence with fewer than 4 tokens
    """
    if tokens:
        if len(tokens) == 0:
            return False
        # Checks 1 paragraph and no sentences
        elif len(tokens) == 1 and len(tokens[0]) == 0:
            return False
        # Checks for length of first paragraph first sentence if only single sentence
        elif len(tokens) == 1 and len(tokens[0]) == 1 and len(tokens[0][0]) < tok_len:
            return False
        return True
    else:
        return n_chars > char_len


def filter_english_paragraphs(
    paragraphs: Sequence[str],
    probability_threshold: float = 0.7,
    proportion_threshold: float = 0.25,
) -> Tuple[List[str], List[Tuple[int, float, float, str]]]:
    filtered_paragraphs = []
    removed_paragraphs = []
    for par_num, paragraph in enumerate(paragraphs):
        cld3_predictions = language_id(paragraph)
        if "eng" in cld3_predictions and (
            (
                cld3_predictions["eng"]["probability"] > probability_threshold
                and cld3_predictions["eng"]["proportion"] > proportion_threshold
            )
            or len(cld3_predictions) == 1
        ):
            removed_paragraphs.append(
                (
                    par_num,
                    cld3_predictions["eng"]["probability"],
                    cld3_predictions["eng"]["proportion"],
                    paragraph,
                )
            )
        else:
            # paragraph is being thrown out, write info to file
            filtered_paragraphs.append(paragraph)
    return filtered_paragraphs, removed_paragraphs


def write_removed_paragraphs(
    filename: str,
    removed_paragraphs: List[Tuple[int, float, float, str]],
    removed_outdir: str,
) -> None:
    os.makedirs(removed_outdir, exist_ok=True)
    with open(os.path.join(removed_outdir, filename), "w", encoding="utf8") as outfile:
        for num, prob, prop, paragraph in removed_paragraphs:
            # To keep tsv from breaking
            clean_paragraph = paragraph.replace("\t", " ").replace("\n", " ")
            print(f"{num}\t{prob}\t{prop}\t{clean_paragraph}", file=outfile)


def extract_document(
    json_doc: Dict,
    outdir: str,
    # segmenters: Dict[str, Segmenter],
    # tokenizers: Dict[str, Tokenizer],
    segmenter: Optional[Segmenter],
    tokenizer: Optional[Tokenizer],
    cuda_id=None,
) -> Tuple[Optional[Segmenter], Tokenizer]:

    url = urllib.parse.unquote(json_doc.get("url", ""))
    modified_url = (
        url.replace(".net/", ".com/")
        .replace(".gov/", ".com/")
        .replace(".org/", ".com/")
    )
    if len(modified_url.split(".com/")) == 2:
        domain, filename = modified_url.split(".com/")
        domain = (
            domain.replace("www.", "")
            .replace("https://", "")
            .replace("http://", "")
            .replace(".", "_")
        )
        filename = filename.replace("/", "_").strip(".html")
        source = json_doc.get("sitemap_prov", {}).get("sitemap", {}).get("url")
        iso = json_doc.get("sitemap_prov", {}).get("sitemap", {}).get("iso")
        html = json_doc.get("original_html")
        utag_data = json_doc.get("utag_data", {})
        page_type = utag_data.get("content_type")
        section = utag_data.get("section", None)
        published_timestamp = json_doc.get("date_published")
        modified_timestamp = json_doc.get("date_modified")
        scraped_timestamp = json_doc.get("time_retrieved")
        authors = json_doc.get("authors")
        keywords = json_doc.get("keywords", [])

        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.getText()
        if iso == "kor":
            title = title.strip("| Voice of America - Korean")

        paragraphs = extract_text(soup, iso)
        if iso == "eng":
            filtered_paragraphs = paragraphs
            removed_paragraphs: List[Tuple[int, float, float, str]] = []
        else:
            filtered_paragraphs, removed_paragraphs = filter_english_paragraphs(
                paragraphs
            )

        n_paragraphs = len(filtered_paragraphs)
        n_chars = sum(len(char) for char in filtered_paragraphs)

        parallel = None
        # Field contains parallel articles for LAO, but holds unneeded text in other languages
        if iso == "lao":
            for p in soup.find_all("a", class_="wsw__a", href=True):
                parallel = p["href"]

        text_joined = " ".join(filtered_paragraphs)
        cld3_predictions = language_id(text_joined)

        predicted_language = predict_language(iso, cld3_predictions)
        cld3_detected_languages = cld3_predictions
        # Always show cld3 predictions if we're filtering it out anyhow
        if predicted_language not in {"eng", "mul"}:
            cld3_detected_languages = filter_confident_langs(
                cld3_predictions,
                proportion_threshold=0.005,
                probability_threshold=0.05,
            )

        if removed_paragraphs:
            removed_outdir = os.path.join(
                outdir, iso + "_" + domain, "eng-filtered-paragraphs"
            )
            write_removed_paragraphs(filename, removed_paragraphs, removed_outdir)

        if iso in SEGMENTABLE_LANGUAGES and (
            segmenter is None or segmenter.language != iso
        ):
            segmenter = setup_segmenter(iso, cuda_id)
        elif iso not in SEGMENTABLE_LANGUAGES:
            segmenter = None
        # segmenter = segmenters[iso] if iso in segmenters else segmenters["xx"]
        # Tokens are List[List[List[str]]]
        # Paragraphs of sentences of tokens
        # this is so we can preserve paragraph splits

        if segmenter:
            sentences: Optional[List[List[str]]] = [
                segmenter.segment(paragraph) for paragraph in filtered_paragraphs
            ]
        else:
            sentences = None

        # Filter out empty sentences
        if sentences:
            sentences = [sent for sent in sentences if sent]

        if iso in TOKENIZABLE_LANGUAGES and (
            tokenizer is None or tokenizer.language != iso
        ):
            tokenizer = setup_tokenizer(iso)
        elif iso not in TOKENIZABLE_LANGUAGES:
            tokenizer = None
        if sentences and tokenizer:
            tokens: Optional[List[List[List[str]]]] = [
                [tokenizer.tokenize(sent) for sent in paragraph]
                for paragraph in sentences
            ]
        else:
            tokens = None

        n_tokens = (
            sum([len(sent) for para in tokens for sent in para]) if tokens else None
        )
        n_sentences = sum([len(para) for para in sentences]) if sentences else None

        output_doc = Document(
            filename=filename,
            url=url,
            url_origin=source,
            content_type=page_type,
            site_language=iso,
            time_published=published_timestamp,
            time_modified=modified_timestamp,
            time_retrieved=scraped_timestamp,
            title=title,
            authors=authors,
            paragraphs=filtered_paragraphs,
            n_paragraphs=n_paragraphs,
            n_chars=n_chars,
            parallel_english_article=parallel,
            # Be generous in showing what languages CLD3 picked up
            cld3_detected_languages=cld3_detected_languages,
            predicted_language=predicted_language,
            sentences=sentences,
            tokens=tokens,
            n_tokens=n_tokens,
            n_sentences=n_sentences,
            keywords=keywords,
            section=section,
        )

        # To avoid invalid outdir
        if page_type is None:
            page_type = "other"
        output_directory = os.path.join(outdir, iso + "_" + domain, page_type)
        os.makedirs(output_directory, exist_ok=True)

        try:
            write_json_doc(
                filename,
                output_doc,
                iso=iso,
                domain=domain,
                outdir=outdir,
            )
        except OSError as exc:
            # Handles file name too long error
            if exc.errno == 63:
                filename_short = filename[-100:]

                write_json_doc(
                    filename_short,
                    output_doc.update_filename(filename_short),
                    iso=iso,
                    domain=domain,
                    outdir=outdir,
                )
        print(f"Writing {filename} to {outdir}")
    else:
        print("Filename processing error: " + url)

    return segmenter, tokenizer


def confident_single_language(
    cld3_dict: Dict[str, Dict[str, Union[str, float]]]
) -> Optional[str]:
    # Check all the languages, if we have probability and proportion above .90
    # return that language iso code
    confident_langs = [
        lang
        for lang in cld3_dict
        if float(cld3_dict[lang]["probability"]) > 0.9
        and float(cld3_dict[lang]["proportion"]) > 0.9
    ]

    if len(confident_langs) == 1:
        return confident_langs[0]
    else:
        return None


def write_json_doc(
    filename: str,
    output_doc: Document,
    *,
    iso: str,
    domain: str,
    outdir: str,
):
    # To avoid invalid outdir
    page_type = "other" if output_doc.content_type is None else output_doc.content_type
    output_directory = os.path.join(outdir, iso + "_" + domain, page_type)
    os.makedirs(output_directory, exist_ok=True)
    if (
        (output_doc.predicted_language == "eng" and iso != "eng")
        or output_doc.predicted_language == "mul"
        or (
            iso == "eng"
            and confident_single_language(output_doc.cld3_detected_languages) != "eng"
        )
    ):
        output_directory = os.path.join(outdir, iso + "_" + domain, "lang_id_filtered")
        os.makedirs(output_directory, exist_ok=True)
        with open(
            os.path.join(output_directory, filename) + ".json",
            "w",
            encoding="utf-8",
        ) as out_file:
            json.dump(output_doc.to_dict(), out_file, ensure_ascii=False)
    elif reasonable_len(output_doc.tokens, output_doc.n_chars, tok_len=10):
        with open(
            os.path.join(output_directory, filename) + ".json",
            "w",
            encoding="utf-8",
        ) as out_file:
            json.dump(output_doc.to_dict(), out_file, ensure_ascii=False)
    else:
        # Note these aren't "empty output" per se but are filtered out
        #   for having essentially no extractable content
        with open(
            os.path.join(output_directory, "empty_output") + ".txt",
            "a",
            encoding="utf-8",
        ) as out_file:
            out_file.write(output_doc.url + "\n")


def language_id(text: str) -> Dict[str, Dict[str, Any]]:
    languages: Dict[str, Dict[str, str]] = {}
    for prediction in cld3.get_frequent_languages(text, num_langs=5):
        language = prediction.language
        if language.endswith("-Latn"):
            language = language[:-5]
        if len(language) == 2 and pycountry.languages.get(alpha_2=language):
            language = pycountry.languages.get(alpha_2=language).alpha_3
        languages[language] = {}
        languages[language]["cld3_language"] = prediction.language
        languages[language]["probability"] = prediction.probability
        languages[language]["is_reliable"] = prediction.is_reliable
        languages[language]["proportion"] = prediction.proportion
    return languages


def extract_text(soup, iso) -> List[str]:
    text = []

    intro = soup.find_all("div", class_="intro")
    for i in intro:
        p_tag = i.find_all("p")
        for p in p_tag:
            split_p = p.getText().split("\n")
            text_intro = [
                intro_paragraph
                for s in split_p
                if is_valid(intro_paragraph := s.strip() and s.strip())
            ]
            text.extend(text_intro)

    article = soup.find_all("div", attrs={"id": "article-content"})
    for a in article:
        if comments := a.find(class_="comments"):
            # Remove comments from tree
            comments.extract()

        if dateline := a.find("span", class_="dateline"):
            text.append(dateline.getText(strip=True))

        p_tag = a.find_all("p")
        # Assumes SNA is already split into paragraphs
        if iso == "sna":
            text_article = [
                sna_paragraph
                for p in p_tag
                if is_valid(
                    sna_paragraph := p.getText(strip=True).strip().replace("\n", " ")
                )
            ]
            text.extend(text_article)
        else:
            for p in p_tag:
                split_p = p.getText().split("\n")
                text_article = [
                    article_paragraph
                    for s in split_p
                    if is_valid(article_paragraph := s.strip() and s.strip())
                ]
                text.extend(text_article)

        if not p_tag:
            wsw_class = a.find_all("div", class_="wsw")
            for w in wsw_class:
                split_w = w.getText().split("\n")
                text_article = [
                    paragraph_text
                    for s in split_w
                    if is_valid(paragraph_text := s.strip() and s.strip())
                ]
                text.extend(text_article)

    article2 = soup.find_all("div", class_="article__content")
    for a in article2:
        p_tag = a.find_all("p")
        for p in p_tag:
            split_p = p.getText(strip=True).split("\n")
            text_article = [
                paragraph for s in split_p if is_valid(paragraph := s.strip())
            ]
            text.extend(text_article)

    return text


def is_valid(text: str) -> bool:
    """
    Simple check to eliminate and filter obviously bad text in paragraph tags.
    """
    text = text.strip()
    text = " ".join(text.split())
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
    elif text.startswith("The code has been copied to your clipboard"):
        return False
    elif text.startswith("The URL has been copied to your clipboard"):
        return False
    elif text.startswith("Embed"):
        return False
    elif text.startswith("0:"):
        return False
    elif text.startswith("share"):
        return False
    elif text.startswith("Telegram Banner"):
        return False
    # AMH, "Listen to the list from the attached audio file."
    elif text.startswith("ዝርዝሩን ከተያያዘው የድምጽ ፋይል ያድምጡ፡፡"):
        return False
    # LAO, "Read more in English"
    elif text.startswith("ອ່ານຂ່າວນີ້ຕື່ມເປັນພາສາອັງກິດ"):
        return False
    # TIR, "The full content can be heard here"
    elif text.startswith("ምሉእ ትሕዝቶ ኣብዚ ምስማዕ ይክኣል::"):
        return False
    # UKR, "See also:"
    elif text.startswith("Дивіться також:"):
        return False
    # UZB, "Voices of America -"
    elif text.startswith('"Amerika Ovozi" -'):
        return False
    elif text.startswith("Avec Reuters"):
        return False
    elif text.startswith("Avec AFP"):
        return False
    # POR, "Click here to open program"
    elif text.startswith("Clique aqui para ouvir"):
        return False
    elif text.startswith("- Clique aqui para ouvir"):
        return False
    elif text.startswith("- Clique para ouvir"):
        return False
    elif text.startswith("-Clique para ouvir"):
        return False
    elif text.startswith("Clique na barra sobre este texto"):
        return False
    else:
        return True


def _process_paths(
    queue: JoinableQueue,
    worker_id: int,
    outdir: str,
) -> None:
    print(f"Starting worker {worker_id}")
    # Segmenters and tokenizers get setup based on language in extract_document
    segmenter = None
    tokenizer = None
    while True:
        batch = queue.get()
        for path in sorted(batch):
            with open(path) as file:
                json_doc = json.load(file)
            segmenter, tokenizer = extract_document(
                json_doc, outdir, segmenter, tokenizer, cuda_id=worker_id % 2
            )
        queue.task_done()


def _process_jsondocs(
    queue: JoinableQueue,
    worker_id: int,
    outdir: str,
    # tokenizers: Dict[str, Tokenizer],
) -> None:
    print(f"Starting worker {worker_id}")
    # Segmenters and tokenizers get setup based on language in extract_document
    segmenter = None
    tokenizer = None
    while True:
        batch = queue.get()
        for json_doc in batch:
            segmenter, tokenizer = extract_document(
                json_doc, outdir, segmenter, tokenizer, cuda_id=worker_id % 2
            )
        queue.task_done()


@cli.command()
@click.argument("inputdir")
@click.argument("outputdir")
@click.option("--n-workers", type=int, default=1)
@click.option("--batchsize", type=int, default=100)
def fromfiles(inputdir, outputdir, n_workers, batchsize):
    multiprocessing.set_start_method("spawn")
    queue: JoinableQueue = JoinableQueue()
    workers = [
        Process(target=_process_paths, args=(queue, i, outputdir))
        for i in range(n_workers)
    ]
    for worker in workers:
        worker.daemon = True
        worker.start()

    # Batches of paths sent to queue
    # haven't actually tested if this is faster than non-batched
    batch = []
    path_count = 0
    batch_count = 0
    for path in find_docpaths(inputdir):
        batch.append(path)
        path_count += 1
        if len(batch) == batchsize:
            queue.put(batch)
            batch_count += 1
            batch = []

    # Add final batch
    if batch:
        queue.put(batch)
        batch_count += 1

    print(f"Added {path_count} articles to queue in {batch_count} batches")
    queue.join()
    print("All queue tasks complete")


@cli.command()
@click.argument("outputdir", type=click.Path(dir_okay=True))
@click.option("--port", default=27200, type=int)
@click.option("--n-extractors", type=int, default=1)
@click.option("--n-db-queriers", type=int, default=1)
@click.option("--batchsize", type=int, default=100)
@click.option("--languages", "-l", multiple=True, type=str)
@click.option(
    "--filemap", type=str, help="Filemap to get language codes for all languages"
)
@click.option("--start-date", type=str, help="%Y-%m-%d", default=None)
@click.option("--end-date", type=str, help="%Y-%m-%d", default=None)
def fromdb(
    outputdir: str,
    n_extractors: int,
    batchsize: int,
    n_db_queriers: int,
    languages: Tuple[str, ...],
    filemap: str,
    port: int = 27200,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    multiprocessing.set_start_method("spawn")

    m = multiprocessing.Manager()
    # TODO make maxsize argument instead of hardcoded
    queue = m.Queue(maxsize=1000)

    workers = [
        Process(target=_process_jsondocs, args=(queue, i, outputdir))
        for i in range(n_extractors)
    ]
    for worker in workers:
        worker.daemon = True
        worker.start()

    languages = languages if languages else languages_from_filemap(filemap)
    date_query = create_date_query(start_date, end_date)
    enqueue_json_docs(
        queue,
        languages,
        n_processes=n_db_queriers,
        batchsize=batchsize,
        port=port,
        date_query=date_query,
    )


if __name__ == "__main__":
    cli()
