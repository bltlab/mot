import os
import json
from pathlib import Path
from typing import Counter, List, Tuple

import click

UNIT_PARAGRAPHS = "paragraphs"
UNIT_SENTENCES = "sentences"
UNIT_TOKENS = "tokens"
VALID_UNITS = (UNIT_PARAGRAPHS, UNIT_SENTENCES, UNIT_TOKENS)
VALID_CONTENT_TYPES = ("article", "audio", "photo", "video")


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.argument("units", type=str)
@click.argument("source", type=click.Path(exists=True))
@click.argument("output_dir")
@click.option(
    "--max-files",
    default=0,
    type=int,
    help="maximum number of files to extract to text (default: unlimited)",
)
@click.option(
    "--max-per-file",
    default=0,
    type=int,
    help="number of sentences or paragraphs per file that you want to extract to text (default: unlimited)",
)
@click.option(
    "--types",
    default="",
    help="content types to extract, specified joined by comma (e.g. --types article,video)."
    + "By default, all content types are extracted.",
)
@click.option(
    "--include-title", is_flag=True,
   # type=bool,
   # default=False,
    help="whether to include the title at the top of the text file. (default: false)",
)
@click.option(
    "--include-authors", is_flag=True,
    help="whether to include the authors at the top of the text file. (default: false)",
)
def extract(
    units: str,
    source: Path,
    output_dir: Path,
    max_files: int = 0,
    max_per_file: int = 0,
    types: str = "",
    include_title: bool = False,
    include_authors: bool = False,
) -> None:
    """Extract json documents into text files in the output directory.\n
    Parameters\n
    ----------\n
    units : string\n
        must choose from the strings: {sentences, paragraphs, tokens} (not case sensitive), chooses how to extract the data\n
    source : path\n
        the directory of json files to extract from or a text file containing paths to json files extract from\n
    output_dir : path\n
        the name of the folder created for housing the new text files.\n
    max_files : int, optional\n
        the number of files to extract from\n
    max_per_file : int, optional\n
        the number of sentences (units is sentences or tokens) or paragraphs to extract from each file\n
    types : string, optional\n
        The content types to extract from.\n
    include_title : boolean, optional\n
        boolean value of whether to include the title at the top of each file\n
    include_authors : boolean, optional\n
        boolean value of whether to include the authors at the top of each file\n

    Raises\n
    ------\n
    Exception\n
        Prints a message indicating that the output dir already existed on your system.\n

    Returns\n
    -------\n
    None.
    """
    # Validate units
    units = units.lower()
    if units not in VALID_UNITS:
        raise ValueError(f"Unknown unit {repr(units)}. Choices are {VALID_UNITS}")

    if units == UNIT_TOKENS and include_title:
        # Titles are not tokenized yet in MOT
        raise ValueError("Titles cannot be included if unit is tokens")

    if os.path.isfile(source):
        # This checks if user is applying a filter (a text file with paths in it), then if they are,
        # uses the helper function _extract_filtered
        _extract_filtered(
            units,
            source,
            output_dir,
            max_files,
            max_per_file,
            include_title,
            include_authors,
        )
    else:
        content_types = _parse_types(types, source)
        # If the user gives types, we use those. If not, we look at all subdirectories of the source
        # directory
        os.makedirs(output_dir, exist_ok=True)
        files_extracted = 0
        for content_type in content_types:
            cur_dir = Path(source) / Path(content_type)
            files = _list_files(cur_dir)
            output_type_dir = Path(output_dir) / Path(content_type)
            output_type_dir.mkdir(exist_ok=True)
            for file in files:
                if max_files and files_extracted >= max_files:
                    break
                filetype = os.path.splitext(file)[-1]
                if filetype == ".json":
                    data = _read_json(file)
                    _make_text_file(
                        output_dir,
                        content_type,
                        data,
                        include_title,
                        include_authors,
                        units,
                        max_per_file,
                    )
                files_extracted += 1


@click.argument("keyword")
@click.argument("file-name")
@click.argument("output-dir")
@click.argument("source", type=click.Path(exists=True))
@click.option(
    "--types",
    default="",
    help="Which content types to extract, specify with a comma and no spaces. "
    "If nothing is selected, defaults to all available content types.",
)
@cli.command()
def search(
    source: Path, output_dir: str, file_name: str, keyword: str, types: str = ""
) -> list:
    """Search for json files with the keyword string in source.\n
    Parameters\n
    ----------\n
    source : path\n
        The folder to search through
    output_dir : path\n
        The folder into which the text file will be deposited\n
    file_name : string\n
        The name of the output text file\n
    keyword : string\n
        The keyword to search for\n
    types : string, optional\n
        Which content types to choose from for the search, defaults to all available content types.\n

    Returns\n
    -------\n
    relevant : list\n
        A list of dict-likes of all the relevant articles.
    """

    content_types = _parse_types(types, source)
    relevant = []
    with open(
        Path(output_dir) / Path(file_name).with_suffix(".txt"), "w", encoding="utf8"
    ) as text:
        for content_type in content_types:
            files = _list_files(Path(source) / Path(content_type))
            for file in files:
                filetype = file.suffix
                if filetype == ".json":
                    data = _read_json(file)
                    if keyword in data["keywords"]:
                        relevant.append(data)
                        print(os.path.abspath(file), file=text)
    return relevant


def _keywords_and_authors(input_dir: Path) -> Tuple[Counter[str], Counter[str]]:
    """Return a tuple of the lists of all keywords. authors in input_dir"""
    files = _list_files(input_dir)
    keywords: Counter[str] = Counter()
    authors: Counter[str] = Counter()
    for file in files:
        filetype = os.path.splitext(file)[-1]
        if filetype == ".json":
            data = _read_json(file)
            for keyword in data["keywords"]:
                keywords[keyword] += 1
            if "authors" in data.keys():
                for author in data["authors"]:
                    authors[author] += 1
    return keywords, authors


def _extract_filtered(
    units: str,
    source: Path,
    output_dir: Path,
    num_files: int,
    max_per_file: int,
    include_title: bool,
    include_authors: bool,
    content_type: str = "",
) -> None:
    """This is a helper function for extract, it only acts when there is a filter file input for source."""
    os.makedirs(output_dir, exist_ok=True)
    num_extracted = 0
    with open(source, encoding="utf8") as f:
        contents = f.readlines()
        for line in contents:  # Checks each line of the text file for a path
            if num_files and num_extracted >= num_files:
                break
            line = line.rstrip("\n")
            data = _read_json(Path(line))
            _make_text_file(
                output_dir,
                content_type,
                data,
                include_title,
                include_authors,
                units,
                max_per_file,
            )
            num_extracted += 1


def _read_json(filename: Path) -> dict:
    """Reads a json file, outputting a dictionary-like of the information about the object"""
    with open(filename, "r", encoding="utf8") as read_file:
        data = json.load(read_file)
    return data


def _make_text_file(
    output_dir: Path,
    content_type: str,
    data: dict,
    include_title: bool,
    include_authors: bool,
    units: str,
    max_per_file: int,
) -> None:
    """Makes and populates the text files from calling extract."""
    filename_txt = Path(data["filename"]).with_suffix(".txt")
    # If the content type isn't specified, read it from the JSON
    if not content_type:
        content_type = data["content_type"]
    content_type_subfolder = Path(output_dir) / Path(content_type)
    os.makedirs(content_type_subfolder, exist_ok=True)
    output_path = content_type_subfolder / Path(filename_txt)
    with open(
        output_path,
        "w",
        encoding="utf8",
    ) as text_file:
        processed = 0
        title = data["title"]
        authors = data["authors"]

        print_title = include_title and title
        if print_title:
            print(title, file=text_file)

        print_authors = include_authors and authors
        if print_authors:
            for author in authors:
                print(author, file=text_file)  # Prints each author on a new line

        # Print blank link after title and/or authors if needed
        if print_title or print_authors:
            print(file=text_file)

        if units == UNIT_TOKENS:
            # Some files don't have tokenization. This should be consistent within a language, so
            # if there aren't tokens, we should crash and explain to the user.
            try:
                tokens = data["tokens"]
            except KeyError:
                raise ValueError(
                    f"Tokens not present in source for file {filename_txt}. "
                    "The most likely cause is that you have requested tokens for a language that "
                    "does not have tokenization."
                )
            for paragraph in tokens:
                # The token field is composed of lists representing paragraphs composed of lists
                # representing sentences.
                for sentence in paragraph:
                    print(" ".join(sentence), file=text_file)
                    processed += 1
                    if max_per_file and processed >= max_per_file:
                        return
                # Blank line between paragraphs
                print(file=text_file)
        elif units == UNIT_SENTENCES:
            # TODO: Handle not having sentences
            for paragraph in data["sentences"]:
                # The sentence field is composed of lists representing paragraphs containing lists
                # representing sentences.
                for sentence in paragraph:
                    print(sentence, file=text_file)
                    processed += 1
                    if max_per_file and processed >= max_per_file:
                        return
                # Blank line between paragraphs
                print(file=text_file)
        elif units == UNIT_PARAGRAPHS:
            for paragraph in data["paragraphs"]:
                # The paragraph field is composed of lists representing paragraphs
                print(paragraph, file=text_file)
                # Blank line between paragraphs
                print(file=text_file)
                processed += 1
                if max_per_file and processed >= max_per_file:
                    return
        else:
            raise ValueError(f"Unknown unit {units}, valid values are {VALID_UNITS}")


def _parse_types(types: str, source: Path) -> List[str]:
    subdirs = [filename.name for filename in Path(source).iterdir()]
    if not types:
        # If types is left blank, we take all subdirectories of source
        content_types = subdirs
    else:
        # Parse comma-separated types
        content_types = types.split(",")
        for content_type in content_types:
            # Check against the set of recognized content types, not the actual subdirectories of
            # the source. It's valid to ask for something that may not be in this specific
            # directory, for example, photo in a directory that doesn't have it.
            if content_type not in VALID_CONTENT_TYPES:
                raise ValueError(
                    f"Invalid content type: {content_type}, valid values are {VALID_CONTENT_TYPES}"
                )
    return content_types


def _list_files(directory: Path) -> List[Path]:
    return [entry for entry in directory.iterdir() if entry.is_file()]


if __name__ == "__main__":
    cli()
