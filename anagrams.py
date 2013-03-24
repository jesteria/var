from __future__ import print_function
import argparse
import collections
import itertools
import re
import sys


INVALID_CHARS = r'[^a-z0-9]+'
MIN_WORD_LEN = 4


def generate_anagrams(text, phrase_len, set_len,
                      invalid_chars=INVALID_CHARS, min_word_len=MIN_WORD_LEN):
    """Generate all `phrase_len`-word anagram sets of `set_len` length
    in the given text.

    """
    scrubbed = re.sub(invalid_chars, ' ', text.lower())
    if min_word_len > 1:
        scrubbed = re.sub(
            r' ?\b[^ ]{1,%s}\b' % (min_word_len - 1), '', scrubbed
        ).strip()
    words = set(scrubbed.split(' '))
    anagrams = collections.defaultdict(set)
    for phrase in itertools.combinations(words, phrase_len):
        key = ''.join(sorted(''.join(phrase)))
        collection = anagrams[key]
        phrase_set = set(phrase)
        if not all(phrase_set.isdisjoint(existing) for existing in collection):
            continue
        collection.add(phrase)
        if len(collection) == set_len:
            yield collection


def main(incoming=sys.stdin, outgoing=sys.stdout):
    parser = argparse.ArgumentParser()
    parser.add_argument('--phrase', type=int, default=2)
    parser.add_argument('--set', type=int, default=2)
    args = parser.parse_args()
    for anagram in generate_anagrams(incoming.read(), args.phrase, args.set):
        chunked = (' '.join(phrase) for phrase in anagram)
        print(*chunked, sep=', ', file=outgoing)


if __name__ == "__main__":
    main()
