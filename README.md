# Multilingual Open Text (MOT)
This is the repository for Multilingual Open Text (MOT), a project at Brandeis University. 

MOT was created by Chester Palen-Michel, June Kim, and Constantine Lignos.

If you use the corpus please cite our paper:
```
@article{palen2022multilingual,
  title={Multilingual Open Text 1.0: Public Domain News in 44 Languages},
  author={Palen-Michel, Chester and Kim, June and Lignos, Constantine},
  journal={arXiv preprint arXiv:2201.05609},
  year={2022}
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
vie,
yue
.
Note just for Pashto we provide sentence splits but not yet tokenization.

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
