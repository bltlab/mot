# Multilingual Open Text (MOT)

This is the repository for Multilingual Open Text (MOT), a project of the Broadening Linguistic Technologies (BLT) Lab at Brandeis University.
MOT was created by Chester Palen-Michel, June Kim, and Constantine Lignos.
This work was supported by a 2021 Brandeis University Provost Research Grant.

If you use the corpus please cite our [paper](https://arxiv.org/abs/2201.05609):
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

# Data Release

You can get the latest version of our data by
[downloading the latest release](https://github.com/bltlab/mot/releases/latest).

The data is released in one gzipped tar file per site in the source data.
Each site file is prefixed with an ISO 639-3 code denoting its language.

### Sentence Segmentation and Tokenization
Each json document has `paragraphs` and `n_paragraphs` with the text split based on paragraph
splits in the original html.
For the languages where we provide tokenization and segmentation,
the fields `sentences`, `n_sentences`, `tokens`, and `n_tokens` are also provided.


We are in the process of expanding the languages for which we provide sentence segmentation.
We currently support:
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
pus,
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
Note just for Pashto we provide sentence splits but not yet tokenization.

# Usage

## Overview
The script mot/motext/scripts/motext.py contains two commands that assist in accessing data in the MOT corpus. These are `search` and `extract`. 
For a description of these commands, run ```motext.py --help```:

```

motext.py --help
    Usage: motext.py [OPTIONS] COMMAND [ARGS]...

    Options:
        --help  Show this message and exit.

    Commands:
        extract  Extract the json documents of the desired content types in the...
        search   Search for json files with the keyword string in the source...
  
```


### Extract
#### Extracting given a source directory
The extract command takes in a folder and extracts information from the JSON files, converting into text files. a full call to this function may look like this:
```motext.py extract units source output_dir --num-files (int1) --max-per-file (int2) types (type1,type2, etc.) --include-title (bool1) --include-authors (bool2)```

This will produce a new directory ```output_dir``` containing subdirectories ```output_dir\type1```, ```output_dir\type2``` etc. Extracted files will be sorted by content type in their respective directories.

---
The arguments are as follows:
  ```units```: choose from [sentences, tokens, paragraphs]. Extract will print one sentence/paragraph per line or a sentence of tokens per line.
  ```source```: this is the source directory from which data will be extracted (ex: swh_voaswahili)
  ```output_dir```: This is the folder to which you would like the data extracted. If output_dir does not exist, a new directory will be created.
  ```(optional) num-files```: The total number of files to extract from source.
  ```(optional) max-per-file```: The max number of units to extract per file (the first max-per-file units will be extracted)
  ```(optional) types```: The content types to extract from the source directory (ex: article,audio). By default will take all available. Input with commas and no spaces.
  ```(optional) include-title```: Whether to include the title at the top of each document on its own line
  ```(optional) include-authors```: Whether to include authors below the title, each on their own line. 
---
Say you would like to extract sentences from "swh_voaswahili" of all content types (this will include article, audio, video, and photo) to the directory "output_folder" including titles and authors. 

Run the following:

```motext.py extract sentences swh_voaswahili output_folder --include-title True --include-authors True```

If instead you want one paragraph per line, run:

```motext.py extract paragraphs swh_voaswahili output_folder --include-title True --include-authors True```

If you only want 6 files total, run:

```motext.py extract sentences swh_voaswahili output_folder --num-files 6 --include-title True --include-authors True```

If you want to constrain the number of lines per file to 7, run:

```motext.py extract sentences swh_voaswahili output_folder --max-per-file 7```

If you want all audio and photo content, run:

```motext.py extract sentences swh_voaswahili output_folder --types audio,photo```

Note that content types are separated by a comma and no spaces.
#### Extracting given a source text file
The ```source``` argument of ```extract``` can also be a text file containing paths directly to json files. Most efficiently, this will be a text file produced by the ```search``` function, outlined in the **Search** section below. To use ```extract``` in this way, it is the same syntax as if the text file were a directory. An example of this usage is as follows:

```motext.py extract sentences filter_text_file.txt output_folder```
### Search
The ```search``` function allows the user to produce a text file of paths to json files that are tagged by a keyword. To call ```search```, run:

```motext.py search source output_dir filename keyword --types (type1,type2, etc.)```

Say you would like a list of all articles in swh_voaswahili that are tagged with "Afrika". 

Run:

```motext.py search swh_voaswahili searches_dir afrika_search Afrika```

The list will be stored in ```afrika_search.txt``` in ```searches_dir```. 

Now say you want to constrain the content types you are searching through to only audio and videos. 

Run:

```motext.py search swh_voaswahili searches_dir afrika_search Afrika --types audio,video```

# Code Release
This code release includes all the code 
we used to scrape and extract text from Voice of America.

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
