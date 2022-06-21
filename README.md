# Multilingual Open Text (MOT)

This is the repository for Multilingual Open Text (MOT), a project of the Broadening Linguistic Technologies (BLT) Lab at Brandeis University. MOT was created by Chester Palen-Michel, June Kim, and Constantine Lignos. This work was supported by a 2021 Brandeis University Provost Research Grant.

If you use the corpus please cite [our LREC 2022 paper](https://arxiv.org/abs/2201.05609):
```
@InProceedings{palenmichel-kim-lignos:2022:LREC,
  author    = {Palen-Michel, Chester  and  Kim, June  and  Lignos, Constantine},
  title     = {Multilingual Open Text Release 1: Public Domain News in 44 Languages},
  booktitle      = {Proceedings of the Language Resources and Evaluation Conference},
  month          = {June},
  year           = {2022},
  address        = {Marseille, France},
  publisher      = {European Language Resources Association},
  pages     = {2080--2089},
  abstract  = {We present Multilingual Open Text (MOT), a new multilingual corpus containing text in 44 languages, many of which have limited existing text resources for natural language processing. The first release of the corpus contains over 2.8 million news articles and an additional 1 million short snippets (photo captions, video descriptions, etc.) published between 2001--2022 and collected from Voice of America's news websites. We describe our process for collecting, filtering, and processing the data. The source material is in the public domain, our collection is licensed using a creative commons license (CC BY 4.0), and all software used to create the corpus is released under the MIT License. The corpus will be regularly updated as additional documents are published.},
  url       = {https://aclanthology.org/2022.lrec-1.224}
}
```

# Releases

The latest version of the MOT data can always be found at
[our latest GitHub release](https://github.com/bltlab/mot/releases/latest).

The current release contains 44 languages: Albanian (sqi), Amharic (amh), Armenian (hye), Azerbaijani (aze), Bambara (bam), Bangla (ben), Bosnian (bos), Burmese (mya), Cantonese (yue), Dari (prs), English (eng), French (fra), Georgian (kat), Greek (ell), Haitian Creole (hat), Hausa (hau), Indonesian (ind), Khmer (khm), Kinyarwanda (kin), Korean (kor), Kurdish (kur), Lao (lao), Lingala (lin), Macedonian (mkd), Mandarin (cmn), Northern (nde), Oromo (orm), Pashto (pus), Persian (fas), Portuguese (por), Russian (rus), Serbian (srp), Shona (sna), Somali (som), Spanish (spa), Swahili (swh), Thai (tha), Tibetan (bod), Tigrinya (tir), Turkish (tur), Ukranian (ukr), Urdu (urd), Uzbek (uzb), and Vietnamese (vie).


## Release Layout

The data is released in one gzipped tar file per crawled site in the source data. Each site file is prefixed with an ISO 639-3 code denoting its language.

There are sometimes multiple sites per language. For example, in English (language code `eng`), there's the main news site at https://www.voanews.com/, the editorials site at https://editorials.voa.gov/, and a site for learning English at https://learningenglish.voanews.com/.

## Downloading and Decompressing the Latest Release

All command-line instructions in this section require the `bash` shell and cloning/downloading this repository.

We have provided two scripts to help download and decompress all the data. Since they download all sites (currently 5.6GB compressed), they take a while to run. If you only want a handful of sites, it's probably easiest to download them manually.

The fastest way to download the data is to set up [the GitHub CLI](https://cli.github.com/), which allows for much faster release downloads. Once you have set it up, run `gh_download_latest_relase.sh`.

If you don't have the GitHub CLI available, run `download_latest_relase.sh` instead.

Both of the download scripts place compressed files (one per site) in the `release` directory. To decompress the downloaded files, run `decompress_latest_release.sh`.

### Sentence Segmentation and Tokenization

Each JSON document in the release has `paragraphs` and `n_paragraphs` fields. These contain the text of each website divided by paragraphs and the number of paragraphs, respectively.  For the languages where we provide sentence segmentation and  tokenization, the fields `sentences`, `n_sentences`, `tokens`, and `n_tokens` are also provided.

We are in the process of expanding the languages for which we provide sentence segmentation and tokenization. We currently provide both sentence segmentation and tokenization in the following languages (by ISO 639-3 code):
amh,
cmn,
ell,
eng,
fas,
fra,
hye,
ind,
khm,
kor,
lao,
mya,
por,
prs,
rus,
spa,
srp,
tha,
tir,
tur,
ukr,
urd,
vie, and
yue.

We provide sentence segmentation but not tokenization for the following languages:
aze,
bos,
hat,
hau,
kin,
lin,
mkd,
nde,
orm,
pus,
sna,
som,
sqi,
swh, and
uzb.

# Working with the Data

## Overview

The `motext` script contains two commands that assist in accessing data in the MOT corpus.

To install `motext`, run `pip install motext`. (If you are working with a clone of the repository and want to make changes to `motext`, you can run `pip install -e .` from the root of the clone.)

Currently, two subcommands are supported by this script: `search` and `extract`. For a description of these commands, run `motext --help`:

```
Usage: motext [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  extract  Extract json documents into text files in the output directory.
  search   Search for json files with the keyword string in source.

```

### Extract

#### Extracting given a source directory

The extract command takes in a folder and extracts information from the JSON files, converting into text files. a full call to this function may look like this:
```
motext extract units source output_dir --num-files (int1) --max-per-file (int2) types article,video,photo,audio --include-title --include-authors
```

This will produce a new directory `output_dir` containing subdirectories `output_dir\article`, `output_dir\video` etc. Extracted files will be sorted by content type in their respective directories.

The arguments are as follows:

* `units`: choose from [sentences, tokens, paragraphs]. Extract will print one sentence/paragraph per line or a sentence of tokens per line.
* `source`: this is the source directory from which data will be extracted (ex: swh_voaswahili)
* `output_dir`: This is the folder to which you would like the data extracted. If output_dir does not exist, a new directory will be created.
* `(optional) num-files`: The total number of files to extract from source.
* `(optional) max-per-file`: The max number of units to extract per file (the first max-per-file units will be extracted)
* `(optional) types`: The content types to extract from the source directory (ex: article,audio). By default will take all available. Input with commas and no spaces.
* `(optional) include-title`: Whether to include the title at the top of each document on its own line
* `(optional) include-authors`: Whether to include authors below the title, each on their own line. 

Say you would like to extract sentences from "swh_voaswahili" of all content types (this will include article, audio, video, and photo) to the directory "output_folder" including titles and authors. 

Run the following:

```
motext extract sentences swh_voaswahili output_folder --include-title --include-authors
```

If instead you want one paragraph per line, run:

```
motext extract paragraphs swh_voaswahili output_folder --include-title --include-authors
```

If you only want 6 files total, run:

```
motext extract sentences swh_voaswahili output_folder --num-files 6 --include-title --include-authors
```

If you want to constrain the number of lines per file to 7, run:

```
motext extract sentences swh_voaswahili output_folder --max-per-file 7
```

If you want all audio and photo content, run:

```
motext extract sentences swh_voaswahili output_folder --types audio,photo
```

Note that content types are separated by a comma and no spaces.

#### Extracting given a source text file

The `source` argument of `extract` can also be a text file containing paths directly to json files. Most efficiently, this will be a text file produced by the `search` function, outlined in the **Search** section below. To use `extract` in this way, it is the same syntax as if the text file were a directory. An example of this usage is as follows:

```
motext extract sentences filter_text_file.txt output_folder
```

### Search

The `search` function allows the user to produce a text file of paths to json files that are tagged by a keyword. To call `search`, run:

```
motext search source output_dir filename keyword
```

Say you would like a list of all articles in swh_voaswahili that are tagged with "Afrika". 

Run:

```
motext search swh_voaswahili searches_dir afrika_search Afrika
```

The list will be stored in `afrika_search.txt` in `searches_dir`. 

Now say you want to constrain the content types you are searching through to only audio and videos. 

Run:

```
motext search swh_voaswahili searches_dir afrika_search Afrika --types audio,video
```

---

# Scraping, Extraction, and Creating Releases

This repository contains all the code used to create version 1 of MOT. While we provide
this for transparency, replication, and in case it will be useful to others, we do not
recommend using it due to its complexity. However, documentation for our release creation
process is below.

## Setup
We recommend using a conda environment when working with the codebase.
Use Python 3.8 or higher.
Install dependencies with `pip install -r requirements.txt`. 

You will need to install MongoDB to store scraped documents.
[mongo installation instructions](https://www.mongodb.com/docs/v4.0/tutorial/install-mongodb-on-os-x/).
To start the database: `mongod --dbpath voa-mongodb/ --wiredTigerCacheSizeGB 16 --port 27200`
We specify a specfic path to store the database and a specific port and limit the cache size.

To dump or restore the DB from a past archive:
`mongodump --port 27200 --archive=dump-7.30.21.gz --gzip`
`mongorestore --port 27200 --archive=dump-7.30.21.gz --gzip`
Use `--bypassDocumentValidation` flag if the backedup db doesn't have all documents passing validation.

## Running Scraping and Extraction

Run downloadsitemaps.py to get fresh sitemaps of VOA.
This requires the `voa-domain.tsv` file with the different VOA domains.
`python extraction/downloadsitemaps.py voa-domains.tsv sitemaps-10.27.21 filemap-10.27.21.tsv`
(Sometimes this fails with 503 error, just run again if needed)

There are two ways to scrape. 
You can scrape from scratch or you can scrape with only the new urls after 
comparing with prior sitemaps.

### Updating the scrape with only new urls

Diff the new sitemap with whatever the most recent previous sitemap is.
`python scripts/comparesitemaps.py filemap-8.16.21.tsv filemap-10.27.21.tsv --early-sitemap-dir sitemaps-8.16.21/ --late-sitemap-dir sitemaps-10.27.21/ --outdir sitemap-diff-10.27.21/`

Back up the database if it hasn't been backed up lately:
`mongodump --port 27200 --archive=dump-7.30.21.gz --gzip`

Scrape using the diffed sitemap urls:
`python extraction/scraper.py update sitemap-diff-10.27.21/new_urls-filemap-10.27.21.tsv --port 27200 `

### Scraping from scratch

Skip `comparesitemaps.py` and use 
`python extraction/scraper.py scrape filemap-10.27.21.tsv sitemaps-10.27.21 --port 27200 `

Dump documents:
This step can be skipped if extracting from the mongo database directly.
Skipping this saves a lot of wasted space writing to disk.
`python extraction/dump_documents.py <outdir> <filemap> --n-processes 20`
If including the custom models download them from [custom-ersatz-models](https://github.com/cpalenmichel/custom-ersatz-models)
or train your own models using [Ersatz](https://github.com/rewicks/ersatz) 
and use the flag `--custom-segmentation-dir <custom-models-dir-path>`. 

Run extraction script:
It is currently recommended without GPUs and just use a high number of cpus for extraction.
`extracttext.py` can be run on dumped json documents from the database or 
can be run from the database directly.
Use `fromdb` to query the database directly and do extraction without writing intermediary files.
Use `fromfiles` to run extract text from intermediate json files dumped from the db.

Sample call with parameters that seem ok on our dev machine.
`time python extraction/extracttext.py fromdb ~/mot/extractions-03.02.22/ --port 27200 --n-extractors 50 --n-db-queriers 10 --batchsize 100 --filemap filemap-03.01.22.tsv --start-date 2001-01-01`


## One-off Scripts 

The directory `scripts` contains a number of one-off scripts that we used briefly
but are not part of the main extraction process. 


## Quality Checks

The directory `qualitychecks` contains some scripts for analysis of the corpus.

## Making a new release 

Install `gh` if it isn't already installed. `conda install gh --channel conda-forge`
Login with `gh auth login`. Follow the steps for logging in through a browser.
Create a release draft on github.
`gh release upload <release number> <dir with the final extractions for release>/*.tgz`
Check everything is uploaded and publish the release on github.
