#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import unittest
import util.render
import nlp.preprocessor
import data
from colorama import Fore, Style

class RenderTestCase(unittest.TestCase):
  def setUp(self):
    self.xtr = util.render.XmlTagRemover()

  def test_tag_remove(self):
    raw_text = data.tag_remove_raw
    target_text = data.tag_remove_target
    self.assertEqual(self.xtr.trim(raw_text), target_text, 
                     "xml tag remove failed")

class PreprocessTestCase(unittest.TestCase):
  def setUp(self):
    self.document = data.preprocess_document
    self.wp = nlp.preprocessor.WordPreprocessor()

  def test_lemmatize(self):

    # origin
    origin_words = self.document.rstrip().split(" ")
    # preprocess
    tokens = self.wp.preprocess(self.document, False)
    
    for origin_word, token in zip(origin_words, tokens):
      print("{} -> {}".format(origin_word, token))

    # assert for zip
    self.assertEqual(len(origin_words), len(tokens))


  """
    check 1: highlight stopwords
    check 2: if stop words apply succeed?
  """
  def test_stopwords(self):

    # copy stop words
    stops = self.wp.stops

    # preprocess
    tokens_no_apply_stops = self.wp.preprocess(self.document, False)

    non_stop_words = []
    # check 1
    for word in tokens_no_apply_stops:
      if not self.is_ascii(word) or word not in stops:
        # normal words
        non_stop_words.append(word)
        print(word, end=" ")
      else:
        # stop words, highlight print!
        print(Fore.RED + word, end=" ")
        print(Style.RESET_ALL, end="")

    print("\n")
    # check 2
    tokens_apply_stops = self.wp.preprocess(self.document, True)
    self.assertEqual(tokens_apply_stops, non_stop_words)


  def test_preprocess(self):
    cleaned_words = self.wp.preprocess(data.preprocess_document)

  def is_ascii(self, token):
    try:
      token.decode("ascii")
    except UnicodeDecodeError:
      # non-ascii
      return False
    else:
      # ascii
      return True

if __name__ == "__main__":
  unittest.main()
