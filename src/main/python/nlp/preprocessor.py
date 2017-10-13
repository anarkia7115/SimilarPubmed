#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from nltk import pos_tag, sent_tokenize, WordNetLemmatizer
from nltk.corpus import stopwords, wordnet

class WordPreprocessor(object):

  def __init__(self):
    self.stops = stopwords.words("english")
    self.lemmatizer = WordNetLemmatizer()
    pass

  """
    1. split into sentences
    2. split into words appying rules
    3. remove stopwords
    4. lemmatization
  """
  def preprocess(self, document, stopwords=True):
    tokens = []

    # modify stopwords list
    if stopwords == False:
      stops = []
    else:
      stops = self.stops


    # 1. 
    for sent in sent_tokenize(document):
      # 2. 
      for token, tag in pos_tag(self.tokenize(sent)):
        # check if can be decoded to ascii
        # if not skip 3, 4
        try:
          token.decode("ascii")
        except UnicodeDecodeError:
          sys.stderr.write("non ascii: {}\n".format(token))
          tokens.append(token)
        else:
          # 3. 
          if token not in stops: 
            # 4. 
            lemma = self.lemmatize(token, tag)
            tokens.append(lemma)
          #else: ignore

    # results
    return tokens

  """
    Lemmatization
  """
  def lemmatize(self, token, tag):
    tag = {
        'N': wordnet.NOUN,
        'V': wordnet.VERB,
        'R': wordnet.ADV,
        'J': wordnet.ADJ
    }.get(tag[0], wordnet.NOUN)

    return self.lemmatizer.lemmatize(token, tag)

  """
    tokenize rules:
      1. split with " "(space)
      2. first word of sentence decapitalize
        a) if only one alphabet, make it lower. 
        b) if all alphabets are capitalized, leave it alone. 
        c) if only first alphabet is capitalized, make it lower. 
      3. remove balances quotes and parentheses (", ', <>, ()) 
      4. if ends with .,:;!? strip
  """
  def tokenize(self, sent):
    # 1. 
    splited_sent = sent.split(" ")

    # 2. 
    word0 = splited_sent[0]
    if len(word0) == 1:
      # rule a)
      word0 = word0.lower()
    elif word0.isupper():
      # rule b)
      pass
    elif word0[0].isupper():
      # rule c)
      word0 = word0.lower()
    #else: unchage

    splited_sent[0] = word0

    tokens = []
    for word in splited_sent:
      # 3. 
      # 4. 
      word = word.strip("{}[]()\"\'<>,.:;!?")
      if (len(word) > 0):
        tokens.append(word)
      else:
        sys.stderr.write("[Warning]lost one word during tokenization\n")

    return tokens

  """
    DEPRECATED
    remove balances quotes and parentheses (", ', <>, ()) 
  """
  def strip_balanced_marks(self, word):

    sys.stderr.write("[Warning]Not using this function any more\n")
    def balanced_marks(word, start_mark, end_mark=None):
      if end_mark == None:
        end_mark = start_mark
      return (word.startswith(start_mark) and word.endswith(end_mark))

    if balanced_marks(word, "(", ")") \
    or balanced_marks(word, "<", ">") \
    or balanced_marks(word, '"') \
    or balanced_marks(word, "'"):
      word = word[1:-1]

    return word
