"""
Script to train sentence piece tokenizer model
"""
from argparse import ArgumentParser

import sentencepiece as spm


def train_sentence_piece():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("input_text")
    parser.add_argument("prefix")
    parser.add_argument("--vocab-size", type=int, default=10000)
    parser.add_argument("--character-coverage", type=float, default=1.0)
    parser.add_argument("--model-type", default="unigram")

    parser.add_argument("--bos-piece", default="<s>")
    parser.add_argument("--eos-piece", default="</s>")
    args = parser.parse_args()
    spm.SentencePieceTrainer.train(
        input=args.input_text,
        model_prefix=args.prefix,
        vocab_size=args.vocab_size,
        bos_piece=args.bos_piece,
        eos_piece=args.eos_piece,
        character_coverage=args.character_coverage,
        model_type=args.model_type,
    )


if __name__ == "__main__":
    train_sentence_piece()
