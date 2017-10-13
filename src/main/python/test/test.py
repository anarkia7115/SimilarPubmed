#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import unittest
import data
from colorama import Fore, Style

class VectorizerTestCase(unittest.TestCase):
  def setUp(self):
    from nlp.vectorizer import WordVectorizer
    self.wv = WordVectorizer()
    from util import reader
    self.read_raw = reader.read_raw

  def test_vectorizer(self):
    input_file = "./data/raw_abs_test.txt"
    # read raw documents from file
    raw_doc_list = self.read_raw(input_file)
    # combine tile and text with " " in between
    doc_list = [(doc_id, "{} {}".format(title, text)) 
                for (doc_id, title, text) in raw_doc_list]
    (term_doc, term_ids) = self.wv.generate_base_mat(doc_list)


    """
      Output to file
    """
    term_doc_out_file = "./data/test_term_doc.txt"
    term_ids_out_file = "./data/test_term_ids.txt"
    self.wv.write_term_doc(term_doc, term_doc_out_file)
    self.wv.write_term_ids(term_ids, term_ids_out_file)

  def test_generate_src_mat(self):
    # term_ids
    term_ids_in_file = "./data/test_term_ids_copy.txt"
    term_ids = self.wv.read_term_ids(term_ids_in_file)

    # doc_lines
    input_file = "./data/raw_abs_test.txt"
    # read raw documents from file
    raw_doc_list = self.read_raw(input_file)
    # combine tile and text with " " in between
    doc_list = [(doc_id, "{} {}".format(title, text)) 
                for (doc_id, title, text) in raw_doc_list]

    # src_mat
    mat_strings = self.wv.generate_src_mat(doc_list, term_ids)
    # write
    src_mat_out_file = "./data/test_src_mat.txt"
    self.wv.write_src_mat(mat_strings, src_mat_out_file)


class RenderTestCase(unittest.TestCase):
  def setUp(self):
    import util.render
    self.xtr = util.render.XmlTagRemover()

  def test_tag_remove(self):
    raw_text = data.tag_remove_raw
    target_text = data.tag_remove_target
    self.assertEqual(self.xtr.trim(raw_text), target_text, 
                     "xml tag remove failed")


class ReaderTestCase(unittest.TestCase):
  def setUp(self):
    from util import reader
    self.read_raw = reader.read_raw

  def test_read_raw(self):
    input_file = "./data/raw_abs_test.txt"
    raw_doc_list = self.read_raw(input_file)
    self.assertEqual(len(raw_doc_list), 100)
    print(raw_doc_list[:3])


class PreprocessTestCase(unittest.TestCase):
  def setUp(self):
    import nlp.preprocessor
    self.wp = nlp.preprocessor.WordPreprocessor()

  def test_preprocess(self):

    raw_text = data.preprocess_raw
    trimed_text = self.wp.preprocess(raw_text)
    target_text = data.preprocess_target

    self.assertEqual(trimed_text, target_text, 
                     "preprocess failed:\n{}\n{}\n".format(trimed_text,
                                                           target_text))


class TokenizeTestCase(unittest.TestCase):
  def setUp(self):
    import nlp.tokenizer
    self.document = data.tokenize_document.decode("utf8")
    self.wt = nlp.tokenizer.WordTokenizer()

  def test_lemmatize(self):

    # origin
    origin_words = self.document.rstrip().split(" ")
    # tokenize
    tokens = self.wt.tokenize(self.document, False)
    
    for origin_word, token in zip(origin_words, tokens):
      print(origin_word.encode("utf8"))
      print(token.encode("utf8"))
      print("{} -> {}".format(origin_word.encode("utf8"), token.encode("utf8")))

    # assert for zip
    self.assertEqual(len(origin_words), len(tokens))


  """
    check 1: highlight stopwords
    check 2: if stop words apply succeed?
  """
  def test_stopwords(self):

    # copy stop words
    stops = self.wt.stops

    # tokenize
    tokens_no_apply_stops = self.wt.tokenize(self.document, False)

    non_stop_words = []
    # check 1
    for word in tokens_no_apply_stops:
      if word not in stops:
        # normal words
        non_stop_words.append(word)
        print(word.encode("utf8"), end=" ")
      else:
        # stop words, highlight print!
        print(Fore.RED + word, end=" ")
        print(Style.RESET_ALL, end="")

    print("\n")
    # check 2
    tokens_apply_stops = self.wt.tokenize(self.document, True)
    self.assertEqual(tokens_apply_stops, non_stop_words)


  def test_tokenize(self):
    cleaned_words = self.wt.tokenize(self.document)

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
