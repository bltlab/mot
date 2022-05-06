#! /bin/bash -eux

INDIR=$1
OUTDIR=$2

# The last 4 exclusions are articles with weird dates
# Remove this logic once dumpdocs also filters pre-2000

tar -cf -  \
 --exclude "./*/404\ error" \
 --exclude "./*/eng-filtered-paragraphs" \
 --exclude "./*/lang_id_filtered" \
 --exclude "./*/poll" \
 --exclude "./*/index" \
 --exclude "./*/quiz" \
 --exclude "./*/other" \
 --exclude "./*/*/empty_output.txt" \
 --exclude "./*/empty_output.txt" \
 --exclude "./*.tgz" \
 --exclude "./eng_voazimbabwe/article/a_a-13-56-74-2009-04-13-voa48-69816822_1459201.json" \
 --exclude "./eng_voazimbabwe/article/a_a-13-56-74-2009-04-13-voa39-69816827_1470821.json" \
 --exclude "./bod_voatibetan/article/a_chinese-scholars-come-to-listen-to-dalai-lama--136276108_1123708.json" \
 --exclude "./eng_learningenglish_voanews/article/a_a-23-2005-10-25-voa7-83033387_117668.json" \
 --exclude "./*/publication_date_too_early" \
  -C $INDIR . | tar -xC $OUTDIR

find $OUTDIR -type d -empty -delete

(cd $OUTDIR && for f in *; do tar -czf $f.tgz $f & done; wait && tar -cf all_domains.tar *.tgz)
