# -*- coding: utf-8 -*-
import os
import json
import click
from pathlib import Path
from collections import Counter
from typing import List


@click.group()
def cli() -> None:
    pass


@click.argument("output_dir")
@click.argument("source", type=click.Path(exists=True))
@click.argument("form", type=str)
@click.option(
    "--num-files",
    default=0,
    type=int,
    help="the number of files in the directory that you want to extract to text (default: unlimited)",
)
@click.option(
    "--max-per-file",
    default=0,
    type=int,
    help="the number of sentences or paragraphs per file that you want to extract to text (default: unlimited)",
)
@click.option(
    "--types",
    default="",
    help="content types to extract, specify with a comma and no spaces (e.g. --types article,video). "
    + "By default, all content types are extracted.",
)
@click.option(
    "--include-title",
    type=bool,
    default=False,
    help="whether to include the title of the article at the top of the text file. (default: False)",
)
@click.option(
    "--include-authors",
    type=bool,
    default=False,
    help="whether to include the authors of the article at the top of the text file. (default: False)",
)
@cli.command()
# TODO: Add full type annotations
def extract(
    form: str,
    source: Path,
    output_dir: Path,
    num_files: int = 0,
    max_per_file: int = 0,
    types: str = "",
    include_title: bool = False,
    include_authors: bool = False,
) -> None:
    """
    Parameters
    ----------
    form : string
        must choose from the strings: {sentence, paragraph, token} (not case sensitive), chooses how to extract the data
    source : path
        the directory of json files to extract from or a text file containing paths to json files extract from
    output_dir : path
        the name of the folder created for housing the new text files.
    num_files : int, optional
        the number of files to extract from
    max_per_file : int, optional
        the number of sentences, paragraphs, or tokens extracted from each file
    types : string, optional
        The content types to extract from.
    include_title : boolean, optional
        boolean value of whether to include the title at the top of each file
    include_authors : boolean, optional
        boolean value of whether to include the authors at the top of each file

    Raises
    ------
    Exception
        Prints a message indicating that the output dir already existed on your system.

    Returns
    -------
    None.

    """
    form = form.lower()
    if form not in ["sentence", "paragraph", "token"]:
        raise ValueError(
            "Must choose sentence, paragraph, or token for form argument"
        )

    if form == "token" and include_title:
        # Titles are not tokenized yet in MOT
        raise ValueError("Titles cannot be included if form is token")

    if os.path.isfile(source):
        # This checks if user is applying a filter (a text file with paths in it), then if they are, uses the helper function _extract_filtered
        _extract_filtered(
            form,
            source,
            output_dir,
            num_files,
            max_per_file,
            include_title,
            include_authors,
        )
    else:
        content_types = _parse_types(types, source)
        # If the user gives types, we use those. If not, we look at all subdirectories of the source directory
        os.makedirs(output_dir, exist_ok=True)
        files_extracted = 0
        for content_type in content_types:
            cur_dir = Path(source) / Path(content_type)
            files = _file_paths(cur_dir)
            output_type_dir = Path(output_dir) / Path(content_type)
            output_type_dir.mkdir(exist_ok=True)
            for file in files:
                if num_files and files_extracted >= num_files:
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
                        form,
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
    help="Which content types to extract, specify with a comma and no spaces. If nothing is selected, defaults to all available content types.",
)
@cli.command()
def search(
    source: Path, output_dir: str, file_name: str, keyword: str, types: str = ""
) -> list:
    """

    Given a keyword and a source folder, searches for all mentions of that keyword in the source folder, outputs a text file of paths
    Parameters
    ----------
    source : path
        The folder to search through
    output_dir : path
        The folder into which the text file will be deposited
    file_name : string
        The name of the output text file
    keyword : string
        The keyword to search for
    types : string, optional
        Which content types to choose from for the search, defaults to all available content types.

    Returns
    -------
    relevant : list
        A list of dict-likes of all the relevant articles.
    """

    content_types = _parse_types(types, source)
    relevant = []
    with open(
        Path(output_dir) / Path(file_name).with_suffix(".txt"), "w", encoding="utf8"
    ) as text:
        for content_type in content_types:
            files = _file_paths(Path(source) / Path(content_type))
            for file in files:
                filetype = file.suffix
                if filetype == ".json":
                    data = _read_json(file)
                    if keyword in data["keywords"]:
                        relevant.append(data)
                        print(os.path.abspath(file), file=text)
    return relevant


def _keywords_and_authors(input_dir: Path) -> tuple:
    """
    Returns a tuple of the lists of all keywords. authors in input_dir
    :param input_dir: The dir to search through
    """
    files = _file_paths(input_dir)
    keywords: Counter = Counter()
    authors: Counter = Counter()
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
    form: str,
    source: Path,
    output_dir: Path,
    num_files: int,
    max_per_file: int,
    include_title: bool,
    include_authors: bool,
    content_type: str = "",
) -> None:
    """
    This is a helper function for extract, it only acts when there is a filter file input for source.
    """
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
                form,
                max_per_file,
            )
            num_extracted += 1


def _read_json(filename: Path) -> dict:
    """
    Reads a json file, outputting a dictionary-like of the information about the object
    """
    with open(filename, "r", encoding="utf8") as read_file:
        data = json.load(read_file)
    return data


def _make_text_file(
    output_dir: Path,
    content_type: str,
    data: dict,
    include_title: bool,
    include_authors: bool,
    form: str,
    max_per_file: int,
) -> None:
    """
    Makes and populates the text files from calling extract.
    """
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

        if form == "token":
            if not processed:
                # Check the first document for tokenization, if available on the first document it should be consistent across the language.
                if "tokens" not in data.keys():
                    raise ValueError("Tokenization is not available for this language.")
            for paragraph in data["tokens"]:
                # The token field is composed of lists representing paragraphs composed of lists representing sentences.
                for sentence in paragraph:
                    print(" ".join(sentence), file=text_file)
                    processed += 1
                    if max_per_file and processed >= max_per_file:
                        return
                print(file=text_file)
        elif form == "sentence":
            for paragraph in data["sentences"]:
                # The sentence field is composed of lists representing paragraphs containing lists representing sentences.
                for sentence in paragraph:
                    print(sentence, file=text_file)
                    processed += 1
                    if max_per_file and processed >= max_per_file:
                        return
                print(file=text_file)
        elif form == "paragraph":
            for paragraph in data["paragraphs"]:
                # The paragraph field is composed of lists representing paragraphs
                print(paragraph, file=text_file)
                print(file=text_file)
                processed += 1
                if max_per_file and processed >= max_per_file:
                    return
        else:
            raise ValueError(f"Unknown form: {form}, please choose from [paragraph | sentence | token]")


def _parse_types(types: str, source: Path) -> List[str]:
    """
    Given a string of types from user input and a source directory, returns either all content types that the user lists in a list, or all subdirs in the source dir
    """
    subdirs = [os.path.basename(filename) for filename in Path(source).iterdir()]
    if not types:
        content_types = subdirs  # If types is left blank, we take all subdirectories of source
    else:
        content_types = types.split(",")
        for content_type in content_types:
            if content_type not in subdirs:
                raise ValueError("You have selected an invalid content type.")
    return content_types


def _file_paths(directory: Path) -> List[Path]:
    """
    Given a directory, gets a list of file paths from that directory
    """
    return [entry for entry in directory.iterdir() if entry.is_file()]


if __name__ == "__main__":
    cli()
