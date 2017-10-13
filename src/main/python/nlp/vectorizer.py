#!/usr/bin/env python
# -*- coding: utf-8 -*-

class WordVectorizer(object):
  def __init__(self):
    from sklearn.feature_extraction.text import CountVectorizer
    from nlp.preprocessor import WordPreprocessor
    from nlp.tokenizer import WordTokenizer

    self.wp = WordPreprocessor()
    self.wt = WordTokenizer()
    self.vectorizer = CountVectorizer(tokenizer=self.wt.tokenize,
                                      preprocessor=self.wp.preprocess)

  """
    1. get contents, doc_ids from document
    2. vectorize contents
    3. get term_ids from vectorizer.vocabulary_
    4. generate term_doc list from 
        zip(coo.col, coo.row, coo.data)
        coo.col  -> term_id
        coo.row  -> doc_id_rn -> doc_id
        coo.data -> count

    @INPUT:  doc_list: [(doc_id, text), ...]
    @OUTPUT: term_doc: {term_id1: ["doc_id1,count1", ...], ...}
             term_ids: {term: term_id}
  """
  def generate_base_mat(self, doc_list):

    # 1. 
    doc_ids = []
    contents = []
    for (doc_id, raw_text) in doc_list:
      # doc_id
      doc_ids.append(doc_id)

      # contents
      contents.append(raw_text)

    # 2. 
    X = self.vectorizer.fit_transform(contents)
    coo = X.tocoo()

    # 3.
    term_ids = self.vectorizer.vocabulary_

    # 4. 
    term_doc = dict()
    for (term_id, doc_id_rn, count_num) in \
        zip(coo.col, coo.row, coo.data):
      # get doc_id using row number
      doc_id = doc_ids[doc_id_rn]
      # term_id -> doc_id,count_num
      one_dc_string = "{},{}".format(doc_id, count_num)
      if term_id not in term_doc:
        term_doc[term_id] = []

      term_doc[term_id].append(one_dc_string)

    return (term_doc, term_ids)

  """
    1. read (doc_id, text) from doc_list
    2. preprocess and tokenize text
    3. loop in tokens, 
      a) convert tokens to term_id
      c) token_counter[token] += 1
    4. combine tv in string
    5. concat doc_id and tv_string
    6. add to mat_strings
  """
  def generate_src_mat(self, doc_list, term_ids):
    mat_strings = []
    # 1. 
    for (doc_id, raw_text) in doc_list:
      # 2.
      # preprocess
      preprocessed_text = self.wp.preprocess(raw_text)
      # tokenize
      tokens = self.wt.tokenize(preprocessed_text.decode("utf8"))

      # 3.
      token_counter = dict()
      for token in tokens:
        # pass missing token, 
        # no base document contains that token. 
        if token not in term_ids:
          continue
        # else

        # a)
        term_id = term_ids[token]

        # b)
        # accumulation
        token_counter[term_id] = token_counter.get(term_id, 0) + 1

      # 4. 
      tv_string = ""
      for (term_id, count_num) in token_counter.items():
        one_tv_string = "{},{}".format(term_id, count_num)
        tv_string = tv_string + "\t" + one_tv_string

      tv_string = tv_string.rstrip("\t")

      # 5. 
      doc_tvs_string = "{}\t{}\n".format(doc_id, tv_string)
      mat_strings.append(doc_tvs_string)

    return mat_strings

  def read_term_ids(self, term_ids_input_file):
    term_ids = dict()
    with open(term_ids_input_file, 'r') as f:
      for l in f:
        (term, term_id) = l.split("\t")
        term_ids[term] = term_id.strip()

    return term_ids

  def write_src_mat(self, mat_strings, output_filename):
    with open(output_filename, 'w') as fw:
      for line in mat_strings:
        fw.write(line)
    return

  def write_term_doc(self, term_doc, output_filename):
    with open(output_filename, 'w') as fw:
      for (term_id, dv_list) in term_doc.items():
        # combine doc_val list
        dv_string = "\t".join(dv_list)
        # concat term_id db
        print_string = "{}\t{}\n".format(term_id, dv_string)
        fw.write(print_string)
    return

  def write_term_ids(self, term_ids, output_filename):
    with open(output_filename, 'w') as fw:
      for (term, term_id) in term_ids.items():
        # two fields in one line
        fw.write("{}\t{}\n".format(term.encode("utf8"), term_id))
    return


