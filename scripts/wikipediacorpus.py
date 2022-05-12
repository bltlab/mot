"""
Args input file, output file
Creates a corpus from Wikipedia dump file.
Inspired by and based on the tutorial here: https://www.kdnuggets.com/2017/11/building-wikipedia-text-corpus-nlp.html
which was inspired by:
https://github.com/panyang/Wikipedia_Word2vec/blob/master/v1/process_wiki.py
"""

import sys
from argparse import ArgumentParser

from gensim.corpora import WikiCorpus

def make_corpus():

	"""Convert Wikipedia xml dump file to text corpus"""
	parser = ArgumentParser()
	parser.add_argument("in_f")
	parser.add_argument("out_f")
	parser.add_argument("--min-tokens", default=1, type=int)
	parser.add_argument("--token-min-len", default=1, type=int)
	parser.add_argument("--token-max-len", default=25, type=int)
	args = parser.parse_args()

	output = open(args.out_f, 'w')
	wiki = WikiCorpus(
		args.in_f,
		article_min_tokens=args.min_tokens,
		token_min_len=args.token_min_len,
		token_max_len=args.token_max_len
	)

	i = 0
	for text in wiki.get_texts():
		output.write(bytes(' '.join(text), 'utf-8').decode('utf-8') + '\n')
		i = i + 1
		if (i % 10000 == 0):
			print('Processed ' + str(i) + ' articles')
	output.close()
	print('Processing complete!')


if __name__ == '__main__':
	make_corpus()
