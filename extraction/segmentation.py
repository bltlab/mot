import re
import sys
from abc import ABC, abstractmethod
from collections import namedtuple
from io import StringIO
from typing import Sequence, List, Optional, Generator

import laonlp
import pythainlp
import razdel
import stanza
import torch
import parsivar
from amseg import AmharicSegmenter
from attr import attrs
from ersatz.candidates import (
    MultilingualPunctuation,
    Split,
    PunctuationSpace,
    AdditionalMultilingualPunctuation,
)
from ersatz.split import EvalModel
from ersatz.utils import get_model_path

from extraction.utils import SPACE_CHARS_STR

SEGMENTABLE_LANGUAGES = {
    "amh",
    "bod",
    "cmn",
    "ell",
    "eng",
    "fas",
    "fra",
    "hye",
    "ind",
    "kor",
    "khm",
    "lao",
    "lin",
    "mya",
    "nde",
    "por",
    "prs",
    "pus",
    "rus",
    "sna",
    "som",
    "spa",
    "srp",
    "tha",
    "tir",
    "tur",
    "ukr",
    "urd",
    "vie",
}


CUSTOM_ERSATZ_MODELS = {
    "aze",
    "ben",
    "bos",
    "hat",
    "hau",
    "kat",
    "kin",
    "kur",
    "mkd",
    "orm",
    "sqi",
    "swh",
    "uzb",
}

SEGMENTABLE_LANGUAGES = SEGMENTABLE_LANGUAGES.union(CUSTOM_ERSATZ_MODELS)


SPACE_CHAR_REGEX = re.compile(rf"[{SPACE_CHARS_STR}]")


class Segmenter(ABC):
    def __init__(self):
        self.language = "xx"

    @abstractmethod
    def segment(self, texts: str) -> List[str]:
        pass


class NaiveRomanSegmenter(Segmenter):
    """
    Segmenter that splits on language specific punctuation if the split creates
    new sentences that are long enough and if the token before punctuation isn't
    suspiciously short and could be an abbreviation or title.
    abbreviations, titles, after digits (unless is a year 1XXX or 20XX)
    closing parens / quotes

    Shona text needs to preprocess if AZ.AZ then go ahead and add space.
    Then split on usual rules.
    """

    Candidate = namedtuple(
        "Candidate", ("punct_idx", "end_idx", "left_token", "right_token")
    )
    PUNCT = r"((\.+)|([!?]))"
    TARGET = re.compile(PUNCT + r"(([A-Za-z])|(\s*(['`\"”)}\]]*)|(\s+)))")

    def __init__(self, lang: str, min_length=10):
        super(NaiveRomanSegmenter, self).__init__()
        self.language = lang
        self.min_length = min_length
        self.BUFFER = 20

    def _next_candidate(self, text: str) -> Generator[Candidate, None, None]:
        for match in re.finditer(NaiveRomanSegmenter.TARGET, text):
            if match.group(4).isalpha():
                end_idx = match.end(4) - 1
            else:
                end_idx = match.end(4)
            punct_idx = match.end(1) if match.group(1) else match.end(2)
            if punct_idx == -1:
                # Shouldn't be possible since each match should have one of the two punctuations
                raise ValueError(
                    f"Bad NaiveSegmenter candidate match in sent:\n {match.string}"
                )
            left_start = punct_idx - self.BUFFER
            if left_start <= 0:
                left_start = 0
            left_context = text[left_start : punct_idx - 1].split()
            right_context = text[end_idx : end_idx + self.BUFFER].split()
            # Pseudo tokens
            left_token = left_context[-1] if left_context else None
            right_token = right_context[0] if right_context else None
            yield NaiveRomanSegmenter.Candidate(
                punct_idx, end_idx, left_token, right_token
            )

    def segment(self, texts: str) -> List[str]:
        sents = []
        start = 0
        for candidate in self._next_candidate(texts):
            # Check explicitly for -Mnu Ndebele titles
            # This has to be separate from other titles and abbreviations
            # because Ndebele titles with prefixes can be longer than other langs
            #  ex) nguMnu.
            if candidate.left_token and candidate.left_token.endswith("Mnu"):
                continue
            # Skip splitting likely Prof. , Mr. etc ABC.
            if (
                candidate.left_token
                and len(candidate.left_token) <= 4
                # any chars contain upper for Ndebele uMnu.
                and any(char.isupper() for char in candidate.left_token)
            ):
                continue

            # Skip numeric things unless they look like a year (len of 4).
            if (
                candidate.left_token
                and candidate.left_token.isdigit()
                and len(candidate.left_token) != 4
            ):
                continue
            if (
                candidate.right_token
                and candidate.right_token.isdigit()
                and len(candidate.right_token) != 4
            ):
                continue
            # Hacky way to stop splits on .. or ...
            if candidate.left_token and candidate.left_token.endswith("."):
                continue
            # Too risky if sentence not capitalized after punctuation
            if candidate.right_token and not candidate.right_token[0].isupper():
                continue
            new_sent = texts[start : candidate.end_idx]
            if len(new_sent) < self.min_length:
                continue
            stripped_new_sent = new_sent.strip()
            if stripped_new_sent:
                sents.append(stripped_new_sent)
                start = candidate.end_idx
        if start <= len(texts):
            rest = texts[start:].strip()
            if rest.strip():
                sents.append(rest)
        return sents


class TibetanNaiveSegmenter(Segmenter):
    """
    Simple segmenter for Tibetan. Splits text on tibetan punctuation.
    Minimal safeguard to require the created sentences are a minimum length
    ༅།། and །། shouldn't split. Everything other ། is fair game if not too short.
    """

    Candidate = namedtuple("Candidate", ("punct_idx", "left", "right", "leftleft"))

    def __init__(self, min_length: int = 8):
        super(TibetanNaiveSegmenter, self).__init__()
        self.language = "bod"
        self.punct = {
            "\u0f0d",
            "\u0f0e",
        }  # TIBETAN MARK SHAD and TIBETAN MARK NYIS SHAD
        self.min_length = min_length

    def _next_candidate(self, text: str) -> Generator[Candidate, None, None]:
        for idx, ch in enumerate(text):
            if ch in self.punct:
                left = text[idx - 1] if idx - 1 > 0 else None
                leftleft = text[idx - 2] if idx - 2 > 0 else None
                right = text[idx + 1] if idx + 1 < len(text) else None
                yield TibetanNaiveSegmenter.Candidate(idx, left, right, leftleft)

    def segment(self, texts: str) -> List[str]:
        sents = []
        start = 0
        for candidate in self._next_candidate(texts):
            new_sent = texts[start : candidate.punct_idx + 1]
            if (
                len(new_sent) >= self.min_length
                and candidate.left != "\u0f05"  # ༅ YIG MGO SGAB MA
                and candidate.leftleft != "\u0f05"
                and candidate.right not in self.punct
                and (candidate.left is None or not candidate.left.isdigit())
                and (candidate.right is None or not candidate.right.isdigit())
            ):
                stripped_new_sent = new_sent.strip()
                if stripped_new_sent:
                    sents.append(stripped_new_sent)
                    start = candidate.punct_idx + 1
        if start <= len(texts):
            rest = texts[start:].strip()
            if rest.strip():
                sents.append(rest)
        return sents


class StanzaSegmenter(Segmenter):
    """
    General Stanza segmenter
    """

    def __init__(self, lang: str):
        super().__init__()
        self.language = lang
        self.nlp = StanzaSegmenter.setup_stanza(lang)

    def segment(self, text: str) -> List[str]:
        doc = self.nlp(text)
        return [sentence.text for sentence in doc.sentences]

    def tokenize(self, text: str):
        doc = self.nlp(text)
        return [token.text for sentence in doc.sentences for token in sentence.words]

    @staticmethod
    def load_model(stanza_lang: str):
        stanza.download(stanza_lang, processors="tokenize")
        return stanza.Pipeline(stanza_lang, processors="tokenize")

    @staticmethod
    def setup_stanza(lang: str):
        if lang == "hye":
            return StanzaSegmenter.load_model("hy")
        elif lang == "ell":
            return StanzaSegmenter.load_model("el")
        elif lang == "ind":
            return StanzaSegmenter.load_model("id")
        elif lang == "kor":
            return StanzaSegmenter.load_model("ko")
        elif lang == "mya":
            return StanzaSegmenter.load_model("my")
        elif lang == "srp":
            return StanzaSegmenter.load_model("sr")
        elif lang == "por":
            return StanzaSegmenter.load_model("pt")
        elif lang == "ukr":
            return StanzaSegmenter.load_model("uk")
        elif lang == "urd":
            return StanzaSegmenter.load_model("ur")
        elif lang == "vie":
            return StanzaSegmenter.load_model("vi")
        else:
            raise ValueError("We aren't setup to use this language with stanza")


class LaoSegmenter(Segmenter):
    """
    Lao Segmenter using LaoNLP
    """

    def __init__(self):
        super().__init__()
        self.language = "lao"

    def segment(self, text: str) -> List[str]:
        text = SPACE_CHAR_REGEX.sub("", text)
        return [
            sent.strip() for sent in laonlp.tokenize.sent_tokenize(text) if sent.strip()
        ]


class RussianSegmenter(Segmenter):
    """
    Razdel based Russian Segmenter
    """

    def __init__(self):
        super().__init__()
        self.language = "rus"

    def segment(self, rus_str: str) -> List[str]:
        # Razdel does have offsets in its output if we need them later
        return [sent.text for sent in razdel.sentenize(rus_str)]


class PersianSegmenter(Segmenter):
    """
    Persian segmenter using Parsivar. Note: Also does normalization..
    """

    def __init__(self, lang: str):
        super().__init__()
        assert lang in {
            "fas",
            "prs",
        }, f"Can't use Parsivar for non persian language {lang}"
        self.language = lang
        self.normalizer = parsivar.Normalizer()
        self.tokenizer = parsivar.Tokenizer()

    def segment(self, fas_str: str) -> List[str]:
        return self.tokenizer.tokenize_sentences(self.normalizer.normalize(fas_str))


class ThaiSegmenter(Segmenter):
    """
    PyThaiNLP uses a CRF trained on TED dataset as the default segmentation approach.
    """

    def __init__(self):
        super().__init__()
        self.language = "tha"

    def segment(self, thai_str: str) -> List[str]:
        thai_str = SPACE_CHAR_REGEX.sub("", thai_str)
        return [
            sent.strip()
            for sent in pythainlp.tokenize.sent_tokenize(thai_str)
            if sent.strip()
        ]


class GeezSegmenter(Segmenter):
    """
    Essentially amseg is a rule-based sentence segmenter and tokenizer.
    It may be worth shifting to a statistically model eventually.
    """

    def __init__(self, language: str):
        super().__init__()
        sent_punct: List = []
        word_punct: List = []
        self.language = language
        self.segmenter: AmharicSegmenter = AmharicSegmenter(sent_punct, word_punct)

    def segment(self, geez_str: str) -> List[str]:
        return self.segmenter.tokenize_sentence(geez_str)


@attrs(auto_attribs=True)
class ErsatzModel:
    model: EvalModel
    candidates: Split


class ErsatzSegmenter(Segmenter):
    """
    Class for pre-trained ersatz models that come with the package.
    """

    def __init__(
        self,
        iso: str = "xx",
        cuda_id: Optional[int] = None,
        use_gpu: bool = False
    ):
        super().__init__()
        self.language = iso
        self.ersatz_model: ErsatzModel = ErsatzSegmenter.setup_ersatz(
            iso,
            cuda_id,
            use_gpu=use_gpu
        )

    def segment(self, text: str) -> List[str]:
        text = SPACE_CHAR_REGEX.sub("", text)
        return [sent.strip() for sent in self.run_ersatz([text]) if sent.strip()]

    def run_ersatz(self, texts: Sequence[str]) -> List[str]:
        output_file = StringIO()
        output_file = self.ersatz_model.model.split(
            texts, output_file, 16, candidates=self.ersatz_model.candidates
        )
        sents = output_file.getvalue().strip().split("\n")
        return sents

    @staticmethod
    def setup_ersatz(
        iso: str,
        cuda_id: Optional[int] = None,
        use_gpu=False
    ) -> ErsatzModel:
        # Load model manually
        # Use model to split sentences
        if iso == "eng":
            candidates = PunctuationSpace()
        elif iso == "ben":
            candidates = AdditionalMultilingualPunctuation()
        else:
            candidates = MultilingualPunctuation()
        if use_gpu:
            if torch.cuda.is_available():
                if cuda_id:
                    device = torch.device(f"cuda:{cuda_id}")
                else:
                    device = torch.device("cuda")
            else:
                device = torch.device("cpu")
        else:
            device = torch.device("cpu")

        if iso == "eng":
            model_path = get_model_path("en")
        elif iso == "spa":
            model_path = get_model_path("es")
        elif iso == "fra":
            model_path = get_model_path("fr")
        elif iso == "khm":
            model_path = get_model_path("km")
        elif iso == "pus":
            model_path = get_model_path("ps")
        elif iso == "tur":
            model_path = get_model_path("tr")
        elif iso == "cmn":
            model_path = get_model_path("zh")
        elif iso in CUSTOM_ERSATZ_MODELS:
            model_path = get_model_path(iso)
        elif iso == "xx":
            model_path = get_model_path("default-multilingual")
        else:
            model_path = get_model_path("default-multilingual")
        model = EvalModel(model_path)

        model.model = model.model.to(device)
        model.device = device
        return ErsatzModel(model, candidates)


def setup_segmenter(
    iso: str = "xx",
    cuda_id: Optional[int] = None,
    use_gpu: bool = False,
) -> Segmenter:
    """Setup segmenters. Use cuda id to setup ersatz models on multiple gpus if applicable."""
    if iso == "tir":
        return GeezSegmenter("tir")
    elif iso == "amh":
        return GeezSegmenter("amh")
    elif iso == "tha":
        return ThaiSegmenter()
    elif iso == "fas":
        return PersianSegmenter(iso)
    elif iso == "prs":
        return PersianSegmenter(iso)
    elif iso == "rus":
        return RussianSegmenter()
    elif iso == "lao":
        return LaoSegmenter()
    elif iso == "hye":
        return StanzaSegmenter(iso)
    elif iso == "por":
        return StanzaSegmenter(iso)
    elif iso == "ell":
        return StanzaSegmenter(iso)
    elif iso == "ind":
        return StanzaSegmenter(iso)
    elif iso == "kor":
        return StanzaSegmenter(iso)
    elif iso == "srp":
        return StanzaSegmenter(iso)
    elif iso == "ukr":
        return StanzaSegmenter(iso)
    elif iso == "urd":
        return StanzaSegmenter(iso)
    elif iso == "vie":
        return StanzaSegmenter(iso)
    elif iso == "mya":
        return StanzaSegmenter(iso)
    elif iso == "bod":
        return TibetanNaiveSegmenter()
    elif iso in {"lin", "nde", "sna", "som"}:
        return NaiveRomanSegmenter(iso)
    elif iso in CUSTOM_ERSATZ_MODELS:
        return ErsatzSegmenter(iso, use_gpu=use_gpu)
    else:
        return ErsatzSegmenter(iso, cuda_id, use_gpu=use_gpu)


if __name__ == "__main__":
    # Uncomment things for testing
    # segmenter = TibetanNaiveSegmenter()
    segmenter = NaiveRomanSegmenter("nde")
    sents = []
    with open(sys.argv[1], "r", encoding="utf8") as file:
        for line in file:
            sents.extend(segmenter.segment(line))

    with open(sys.argv[2], "w", encoding="utf8") as outfile:
        for sent in sents:
            print(sent, file=outfile)
            print(file=outfile)
    # Testing ground
#     segmenter = setup_segmenter("spa")
#
#     sents = segmenter.segment(
#         "Buenos dias! Como estas? Hay mucha gente aqui. me gusta cafe. Llama a Dra. Cafe. "
#     )
#     print(sents)
#     segmenter = setup_segmenter("tha")
#     sents = segmenter.segment(
#         "Saoirse Ronan เป็นคนหนึ่งที่ได้รับการเสนอชื่อให้เข้ารับรางวัลตุ๊กตาทองสำหรับนักแสดงยอดเยี่ยมฝ่ายหญิงจาก “Brooklyn”"
#     )
#     print(sents)
#
#     from parsivar import Tokenizer, Normalizer
#
#     tmp_text = "به گزارش ایسنا سمینار شیمی آلی از امروز ۱۱ شهریور ۱۳۹۶ در دانشگاه علم و صنعت ایران آغاز به کار کرد. این سمینار تا ۱۳ شهریور ادامه می یابد."
#     my_normalizer = Normalizer()
#     my_tokenizer = Tokenizer()
#     sents = my_tokenizer.tokenize_sentences(tmp_text)
#     print(sents)
#
#     words = my_tokenizer.tokenize_words(tmp_text)
#     print(words)
#     segmenter = StanzaSegmenter("mya")
#     sentences = segmenter.segment(
#         """ျမန္မာႏိုင္ငံမွာ ေဖေဖာ္ဝါရီလ ၁ ရက္ေန႔ စစ္တပ္က အာဏာသိမ္းၿပီးတဲ့ေနာက္ ၿငိမ္းခ်မ္းေရးလုပ္ငန္းစဥ္ ေသဆုံးသြားၿပီလို႔ ႏိုင္ငံတကာ အက်ပ္အတည္းေတြကို ေစာင့္ၾကည့္ေလ့လာေနတဲ့ International Crisis Group (ICG) က ေျပာလိုက္ပါတယ္။
#
# စစ္တပ္က အာဏာသိမ္းလိုက္တာေၾကာင့္ လူထုထိခိုက္ရတဲ့ ပဋိပကၡေတြ အရွိန္ ျမင့္လာၿပီး လူမ်ိဳးစုလက္နက္ကိုင္အဖြဲ႕ အမ်ားအျပားပါဝင္တဲ့ ဆယ္စုႏွစ္တခုၾကာ ၿငိမ္းခ်မ္းေရးလုပ္ငန္းစဥ္ အဆုံးသတ္သြားၿပီလို႔ ICG က ေထာက္ျပထားပါတယ္။
#
# ဒါေၾကာင့္ ႏိုင္ငံတကာက အလႉရွင္ႏိုင္ငံေတြ၊ အဖြဲ႕အစည္းေတြအေနနဲ႔ ၿငိမ္းခ်မ္းေရးလုပ္ငန္းစဥ္ကို အေထာက္အပံ့ျပဳေနတာကေန လက္နက္ကိုင္ပဋိုပကၡျဖစ္ေနတဲ့ ေဒသေတြကလူထုကို ကူညီေထာက္ပံ့ေရးလုပ္ငန္းဘက္ အာ႐ုံေျပာင္းသင့္တယ္လို႔ ဒီကေန႔ ထုတ္ ICG အစီရင္ခံစာသစ္တခုမွာ တိုက္တြန္းထားပါတယ္။"""
#     )
#     for sent in sentences:
#         print(sent)
#         print()
#         tokens = segmenter.tokenize(sent)
#         print(tokens)
#         print()
#         print()
