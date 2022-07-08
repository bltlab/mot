import cld3
import os
import json
import csv
from argparse import ArgumentParser
from multiprocessing import Pool
from typing import Generator

ISO_CLD3_DICT = {
    "eng": "en",
    "sqi": "sq",
    "bos": "bs",
    "ell": "el",
    "mkd": "mk",
    "srp": "sr",
    "ukr": "uk",
    "hye": "hy",
    "kat": "ka",
    "rus": "ru",
    "uzb": "uz",
    "ben": "bn",
    "pus": "ps",
    "urd": "ur",
    "mya": "my",
    "ind": "id",
    "khm": "km",
    "kor": "ko",
    "lao": "lo",
    "tha": "th",
    "vie": "vi",
    "amh": "am",
    "bam": "fr",
    "fra": "fr",
    "hau": "ha",
    "por": "pt",
    "sna": "sn",
    "som": "so",
    "swh": "sw",
    "fas": "fa",
    "kur": "ku",
    "tur": "tr",
    "hat": "ht",
    "spa": "es",
}


def language(path: str, outdir: str) -> None:
    with open(path) as file:
        iso_domain = path.split("/")[1]
        os.makedirs(outdir, exist_ok=True)
        tsv_name = os.path.join(outdir, iso_domain) + ".tsv"

        with open(tsv_name, "a", newline="") as tsv_file:
            fieldnames = [
                "file_name",
                "url",
                "is_supported",
                "paragraph#",
                "num_chars",
                "language1",
                "probability1",
                "unexpected1",
                "language2",
                "probability2",
                "unexpected2",
            ]
            writer = csv.DictWriter(
                tsv_file, fieldnames=fieldnames, restval="", extrasaction="raise"
            )

            data = json.load(file)
            iso = data.get("language_iso_639_3")
            filename = data.get("filename")
            paragraphs = data.get("paragraphs")
            text = " ".join(paragraphs)
            prediction_dictionary = {}

            prediction_dictionary[-1] = {}
            prediction_dictionary[-1]["file_name"] = filename
            prediction_dictionary[-1]["url"] = data.get("url")
            prediction_dictionary[-1]["is_supported"] = is_supported(iso)
            prediction_dictionary[-1]["paragraph#"] = -1
            prediction_dictionary[-1]["num_chars"] = data.get("n_chars")
            for i, prediction in enumerate(
                cld3.get_frequent_languages(text, num_langs=2)
            ):
                prediction_dictionary[-1]["language" + str(i + 1)] = prediction.language
                prediction_dictionary[-1][
                    "probability" + str(i + 1)
                ] = prediction.probability
                prediction_dictionary[-1]["unexpected" + str(i + 1)] = is_unexpected(
                    prediction.language, iso
                )

            for i, paragraph in enumerate(paragraphs):
                if len(paragraphs) > 1:
                    prediction_dictionary[i] = {}
                    prediction_dictionary[i]["file_name"] = filename
                    prediction_dictionary[i]["url"] = data.get("url")
                    prediction_dictionary[i]["is_supported"] = is_supported(iso)
                    prediction_dictionary[i]["paragraph#"] = i
                    prediction_dictionary[i]["num_chars"] = len(paragraph)
                    for j, prediction in enumerate(
                        cld3.get_frequent_languages(paragraph, num_langs=2)
                    ):
                        prediction_dictionary[i][
                            "language" + str(j + 1)
                        ] = prediction.language
                        prediction_dictionary[i][
                            "probability" + str(j + 1)
                        ] = prediction.probability
                        prediction_dictionary[i][
                            "unexpected" + str(j + 1)
                        ] = is_unexpected(prediction.language, iso)

            if is_empty(tsv_name):
                writer.writeheader()
            for num in prediction_dictionary:
                writer.writerow(prediction_dictionary[num])

        print(f"Writing {path} to {outdir}")


def is_unexpected(cld3_code, iso_code):
    if is_supported(iso_code):
        return cld3_code != ISO_CLD3_DICT[iso_code] and cld3_code != "en"


def is_supported(iso_code):
    return iso_code in ISO_CLD3_DICT


def is_empty(file_path):
    return os.path.getsize(file_path) == 0


def find_docpaths(inputdir: str) -> Generator[str, None, None]:
    for root, dirs, files in os.walk(inputdir):
        for file in files:
            if file.endswith(".json"):
                yield os.path.join(root, file)


def run():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputdir",
        help="Input directory containing json in directories by iso/sitename",
    )
    parser.add_argument(
        "outdir", help="Output directory containing tsv by iso/sitename"
    )
    parser.add_argument("--n-workers", type=int, default=1)
    args = parser.parse_args()

    if args.n_workers == 1:
        for path in find_docpaths(args.inputdir):
            language(path, args.outdir)
    else:
        pool = Pool(args.n_workers)
        for path in find_docpaths(args.inputdir):
            pool.apply_async(language, (path, args.outdir))
        pool.close()
        pool.join()


if __name__ == "__main__":
    run()
