#!/usr/bin/env python
# -*- coding: utf-8 -*-

class WordPreprocessor(object):

  def __init__(self):
    import util.render
    self.xtr = util.render.XmlTagRemover()
    pass

  """
  @INPUT:  text
  @OUTPUT: tag_removed_text
  """
  def preprocess(self, text):
    return self.xtr.trim(text).strip().strip('"')

