#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

def generate_base_mat(input_raw_text_file, 
                      output_term_id_file, 
                      output_mat_file):

  from nlp.vectorizer import WordVectorizer
  wv = WordVectorizer()
  from util import reader
  read_raw = reader.read_raw

  # read raw documents from file
  raw_doc_list = read_raw(input_raw_text_file)
  # combine tile and text with " " in between
  doc_list = [(doc_id, "{} {}".format(title, text)) 
              for (doc_id, title, text) in raw_doc_list]
  (term_doc, term_ids) = wv.generate_base_mat(doc_list)

  # output
  wv.write_term_ids(term_ids, output_term_id_file)
  wv.write_term_doc(term_doc, output_mat_file)
  return

def generate_src_mat(input_raw_text_file, 
                     input_term_id_file, 
                     output_mat_file):

  from nlp.vectorizer import WordVectorizer
  wv = WordVectorizer()
  from util import reader
  read_raw = reader.read_raw

  #term_ids
  term_ids = wv.read_term_ids(input_term_id_file)

  # doc_lines
  # read raw documents from file
  raw_doc_list = read_raw(input_raw_text_file)
  # combine tile and text with " " in between
  doc_list = [(doc_id, "{} {}".format(title, text)) 
              for (doc_id, title, text) in raw_doc_list]

  # src_mat
  mat_strings = wv.generate_src_mat(doc_list, term_ids)
  # write
  wv.write_src_mat(mat_strings, output_mat_file)


def main(argv):
  if argv[1] == "base":
    input_raw_text_file = argv[2]
    output_term_id_file = argv[3]
    output_mat_file = argv[4]

    generate_base_mat(input_raw_text_file, 
                      output_term_id_file,
                      output_mat_file) 

  elif argv[1] == "calc":
    input_raw_text_file = argv[2]
    input_term_id_file = argv[3]
    output_mat_file = argv[4]

    generate_src_mat(input_raw_text_file, 
                     input_term_id_file, 
                     output_mat_file)
    

if __name__ == "__main__":
  """
  argv[1] type: base/calc
  argv[2] input_raw_text_file(doc_id, title, text)

  if base:
    argv[3] output_term_id_file
    argv[4] output_mat_file
  elif calc:
    argv[3] input_term_id_file
    argv[4] output_mat_file


  """
  argv = sys.argv
  print("""
        type: {}
        raw_text: {}
        term_id: {}
        mat_file: {}
       """.format(argv[1], argv[2], argv[3], argv[4]))

  main(argv)
