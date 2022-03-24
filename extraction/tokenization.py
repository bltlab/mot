from abc import ABC, abstractmethod
from typing import List, Optional, Callable

import laonlp
import parsivar
import pythainlp
import razdel

from amseg import AmharicSegmenter
from spacy.lang.en import English
from spacy.lang.es import Spanish
from spacy.lang.fr import French
from spacy.lang.ru import Russian
from spacy.lang.tr import Turkish
from spacy.lang.xx import MultiLanguage
from spacy.lang.zh import Chinese
from spacy.tokenizer import Tokenizer

from extraction.segmentation import StanzaSegmenter

TOKENIZABLE_LANGUAGES = {
    "amh",
    "cmn",
    "ell",
    "eng",
    "fas",
    "fra",
    "hye",
    "ind",
    "khm",
    "kor",
    "lao",
    "mya",
    "por",
    "prs",
    "rus",
    "spa",
    "srp",
    "tha",
    "tir",
    "tur",
    "ukr",
    "urd",
    "vie",
    "yue",
}


def load_khmernltk() -> Callable:
    from khmernltk import word_tokenize as khmer_tokenize

    return khmer_tokenize


class BaseTokenizer(ABC):
    """
    Abstract Base class for Tokenizer
    """

    def __init__(self):
        self.language = "xx"

    @abstractmethod
    def tokenize(self, text: str) -> List[str]:
        """
        Split a string of text into tokens
        """
        pass


class StanzaTokenizer(BaseTokenizer):
    """
    Stanza Tokenizer.
    """

    def __init__(self, language: str):
        super().__init__()
        self.language = language
        # Stanza tokenizes and segments all as one pipeline, so reusing segmenter here.
        self.segmenter = StanzaSegmenter(language)

    def tokenize(self, text: str) -> List[str]:
        return self.segmenter.tokenize(text)


class KhmerTokenizer(BaseTokenizer):
    """
    Tokenizer using khmer-nltk
    """

    def __init__(self):
        super().__init__()
        self.language = "khm"
        self.khmer_tokenize = load_khmernltk()

    def tokenize(self, text: str) -> List[str]:
        # This tokenizer doesn't seem bad but is capable of producing whitespace tokens
        # And that's not good, so needs filtered out, ideally replace this one eventually
        return [token for token in self.khmer_tokenize(text) if token.strip()]


class LaoTokenizer(BaseTokenizer):
    """
    Lao tokenizer using laonlp.
    """

    def __init__(self):
        super().__init__()
        self.language = "lao"

    def tokenize(self, text: str) -> List[str]:
        return laonlp.tokenize.word_tokenize(text)


class RussianTokenizer(BaseTokenizer):
    """
    Tokenizer for Russian with Razdel.
    """

    def __init__(self):
        super().__init__()
        self.language = "rus"

    def tokenize(self, text: str) -> List[str]:
        # Razdel provides offsets if we go that route later.
        return [token.text for token in razdel.tokenize(text)]


class PersianTokenizer(BaseTokenizer):
    """
    Tokenizer for Persian Farsi with Parsivar.
    Using for Dari Persian as well.
    """

    def __init__(self, lang: str):
        super().__init__()
        assert lang in {
            "fas",
            "prs",
        }, f"Can't use Parsivar for non persian language {lang}"
        self.language = lang
        self.tokenizer = parsivar.Tokenizer()

    def tokenize(self, text: str) -> List[str]:
        # Assumes text is already normalized by sentence splitting.
        # Though is not strictly necessary for parsivar to work
        return self.tokenizer.tokenize_words(text)


class ThaiTokenizer(BaseTokenizer):
    """
    Tokenizer for Thai using PyThaiNLP.
    """

    def __init__(self):
        super().__init__()
        self.language = "tha"

    def tokenize(self, text: str) -> List[str]:
        return pythainlp.tokenize.word_tokenize(text)


class GeezTokenizer(BaseTokenizer):
    def __init__(self, iso: str):
        super().__init__()
        self.language = iso
        sent_punct: List = []
        word_punct: List = []
        self.segmenter = AmharicSegmenter(sent_punct, word_punct)

    def tokenize(self, text: str) -> List[str]:
        return self.segmenter.amharic_tokenizer(text)


class SpacyTokenizer(BaseTokenizer):
    def __init__(self, lang_code: Optional[str] = None):
        super().__init__()
        self.language = lang_code if lang_code else "xx"
        if lang_code == "eng":
            self.tokenizer = English().tokenizer
        elif lang_code == "cmn":
            zh_nlp = Chinese.from_config(
                {"nlp": {"tokenizer": {"segmenter": "pkuseg"}}}
            )
            # Weirdness in spacy type hints
            zh_nlp.tokenizer.initialize(pkuseg_model="mixed")  # type: ignore
            self.tokenizer = zh_nlp.tokenizer
        elif lang_code == "yue":
            zh_nlp = Chinese.from_config(
                {"nlp": {"tokenizer": {"segmenter": "pkuseg"}}}
            )
            # Weirdness in spacy type hints
            zh_nlp.tokenizer.initialize(pkuseg_model="mixed")  # type: ignore
            self.tokenizer = zh_nlp.tokenizer
        elif lang_code == "fra":
            self.tokenizer = French().tokenizer
        elif lang_code == "spa":
            self.tokenizer = Spanish().tokenizer
        elif lang_code == "rus":
            self.tokenizer = Russian().tokenizer
        elif lang_code == "tur":
            self.tokenizer = Turkish().tokenizer
        else:
            self.tokenizer = MultiLanguage().tokenizer

    def tokenize(self, text: str) -> List[str]:
        tokens = self.tokenizer(text)
        return [t.text for t in tokens]


def setup_tokenizer(iso: str = "xx") -> BaseTokenizer:
    if iso == "eng":
        return SpacyTokenizer("eng")
    elif iso == "cmn":
        return SpacyTokenizer("cmn")
    elif iso == "yue":
        return SpacyTokenizer("yue")
    elif iso == "fra":
        return SpacyTokenizer("fra")
    elif iso == "spa":
        return SpacyTokenizer("spa")
    elif iso == "rus":
        return SpacyTokenizer("rus")
    elif iso == "tur":
        return SpacyTokenizer("tur")
    elif iso == "tha":
        return ThaiTokenizer()
    elif iso == "amh":
        return GeezTokenizer("amh")
    elif iso == "tir":
        return GeezTokenizer("tir")
    elif iso == "fas":
        return PersianTokenizer(iso)
    elif iso == "prs":
        return PersianTokenizer(iso)
    elif iso == "rus":
        return RussianTokenizer()
    elif iso == "lao":
        return LaoTokenizer()
    elif iso == "hye":
        return StanzaTokenizer(iso)
    elif iso == "ell":
        return StanzaTokenizer(iso)
    elif iso == "ind":
        return StanzaTokenizer(iso)
    elif iso == "kor":
        return StanzaTokenizer(iso)
    elif iso == "por":
        return StanzaTokenizer(iso)
    elif iso == "srp":
        return StanzaTokenizer(iso)
    elif iso == "ukr":
        return StanzaTokenizer(iso)
    elif iso == "urd":
        return StanzaTokenizer(iso)
    elif iso == "vie":
        return StanzaTokenizer(iso)
    elif iso == "mya":
        return StanzaTokenizer(iso)
    else:
        return SpacyTokenizer()


def tokenize(sent: str, tokenizer: Tokenizer) -> List[str]:
    tokens = tokenizer(sent)
    return [t.text for t in tokens]


if __name__ == "__main__":
    tokenizer = setup_tokenizer("eng")
    sents = [
        "This, is my sentence.",
        "Esto es otro frase pero con una 'isn't', jajaja!",
    ]
    for s in sents:
        print(tokenize(s, tokenizer))
