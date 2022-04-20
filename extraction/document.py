from typing import NamedTuple, Optional, List, Dict, Union, Any


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
