import re
from abc import ABC, abstractmethod
from io import StringIO
from typing import Sequence, List, Optional

import laonlp
import pythainlp
import razdel
import stanza
import torch
import parsivar
from amseg import AmharicSegmenter
from attr import attrs
from ersatz.candidates import MultilingualPunctuation, Split, PunctuationSpace
from ersatz.split import EvalModel
from ersatz.utils import get_model_path

from extraction.utils import SPACE_CHARS_STR

SEGMENTABLE_LANGUAGES = {
    "amh",
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
    "mya",
    "por",
    "prs",
    "pus",
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


CUSTOM_ERSATZ_MODELS = {
    "aze", "bos", "hat", "hau", "kin", "lin", "mkd", "nde", "orm", "sna", "som", "sqi", "swh", "uzb"
}

SEGMENTABLE_LANGUAGES = SEGMENTABLE_LANGUAGES.union(CUSTOM_ERSATZ_MODELS)


SPACE_CHAR_REGEX = re.compile(rf"[{SPACE_CHARS_STR}]")


class Segmenter(ABC):
    def __init__(self):
        self.language = "xx"

    @abstractmethod
    def segment(self, texts: str) -> List[str]:
        pass


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

    def __init__(self, iso: str = "xx", cuda_id: Optional[int] = None, custom_segmentation_model_path: Optional[str] = None):
        super().__init__()
        self.language = iso
        self.ersatz_model: ErsatzModel = ErsatzSegmenter.setup_ersatz(
            iso, cuda_id, custom_segmentation_model_path
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
    def setup_ersatz(iso: str, cuda_id: Optional[int] = None, custom_model_dir: Optional[str] = None) -> ErsatzModel:
        # Load model manually
        # Use model to split sentences
        if iso == "eng":
            candidates = PunctuationSpace()
        else:
            candidates = MultilingualPunctuation()
        if torch.cuda.is_available():
            if cuda_id:
                device = torch.device(f"cuda:{cuda_id}")
            else:
                device = torch.device("cuda")
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
        elif iso == "yue":
            model_path = get_model_path("zh")
        elif iso in CUSTOM_ERSATZ_MODELS:
            model_path = f"{custom_model_dir}/{iso}.checkpoint.model"
        elif iso == "xx":
            model_path = get_model_path("default-multilingual")
        else:
            model_path = get_model_path("default-multilingual")
        model = EvalModel(model_path)

        model.model = model.model.to(device)
        model.device = device
        return ErsatzModel(model, candidates)


def setup_segmenter(iso: str = "xx", cuda_id: Optional[int] = None,
                    custom_segmentation_model_path: Optional[str] = None) -> Segmenter:
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
    elif iso in CUSTOM_ERSATZ_MODELS:
        return ErsatzSegmenter(iso, custom_segmentation_model_path=custom_segmentation_model_path)
    else:
        return ErsatzSegmenter(iso, cuda_id)


if __name__ == "__main__":

    # Testing ground
    segmenter = setup_segmenter("spa")

    sents = segmenter.segment(
        "Buenos dias! Como estas? Hay mucha gente aqui. me gusta cafe. Llama a Dra. Cafe. "
    )
    print(sents)
    segmenter = setup_segmenter("tha")
    sents = segmenter.segment(
        "Saoirse Ronan เป็นคนหนึ่งที่ได้รับการเสนอชื่อให้เข้ารับรางวัลตุ๊กตาทองสำหรับนักแสดงยอดเยี่ยมฝ่ายหญิงจาก “Brooklyn”"
    )
    print(sents)

    from parsivar import Tokenizer, Normalizer

    tmp_text = "به گزارش ایسنا سمینار شیمی آلی از امروز ۱۱ شهریور ۱۳۹۶ در دانشگاه علم و صنعت ایران آغاز به کار کرد. این سمینار تا ۱۳ شهریور ادامه می یابد."
    my_normalizer = Normalizer()
    my_tokenizer = Tokenizer()
    sents = my_tokenizer.tokenize_sentences(tmp_text)
    print(sents)

    words = my_tokenizer.tokenize_words(tmp_text)
    print(words)
    segmenter = StanzaSegmenter("mya")
    sentences = segmenter.segment(
        """ျမန္မာႏိုင္ငံမွာ ေဖေဖာ္ဝါရီလ ၁ ရက္ေန႔ စစ္တပ္က အာဏာသိမ္းၿပီးတဲ့ေနာက္ ၿငိမ္းခ်မ္းေရးလုပ္ငန္းစဥ္ ေသဆုံးသြားၿပီလို႔ ႏိုင္ငံတကာ အက်ပ္အတည္းေတြကို ေစာင့္ၾကည့္ေလ့လာေနတဲ့ International Crisis Group (ICG) က ေျပာလိုက္ပါတယ္။

စစ္တပ္က အာဏာသိမ္းလိုက္တာေၾကာင့္ လူထုထိခိုက္ရတဲ့ ပဋိပကၡေတြ အရွိန္ ျမင့္လာၿပီး လူမ်ိဳးစုလက္နက္ကိုင္အဖြဲ႕ အမ်ားအျပားပါဝင္တဲ့ ဆယ္စုႏွစ္တခုၾကာ ၿငိမ္းခ်မ္းေရးလုပ္ငန္းစဥ္ အဆုံးသတ္သြားၿပီလို႔ ICG က ေထာက္ျပထားပါတယ္။

ဒါေၾကာင့္ ႏိုင္ငံတကာက အလႉရွင္ႏိုင္ငံေတြ၊ အဖြဲ႕အစည္းေတြအေနနဲ႔ ၿငိမ္းခ်မ္းေရးလုပ္ငန္းစဥ္ကို အေထာက္အပံ့ျပဳေနတာကေန လက္နက္ကိုင္ပဋိုပကၡျဖစ္ေနတဲ့ ေဒသေတြကလူထုကို ကူညီေထာက္ပံ့ေရးလုပ္ငန္းဘက္ အာ႐ုံေျပာင္းသင့္တယ္လို႔ ဒီကေန႔ ထုတ္ ICG အစီရင္ခံစာသစ္တခုမွာ တိုက္တြန္းထားပါတယ္။"""
    )
    for sent in sentences:
        print(sent)
        print()
        tokens = segmenter.tokenize(sent)
        print(tokens)
        print()
        print()
