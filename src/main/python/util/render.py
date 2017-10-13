#!/usr/bin/env python
# encoding: utf-8

import re
import ast
import string

class AbstractSaver:

  """
    1. read json data
    2. get pmid
    3. get abstract
    4. save to file as format:

      26116688|t|Syphilis and gonorrhoea increase sharply in England.
      26116688|a|-No abstract-

  """
  def __init__(self, jsonDatas, outputPath):

    self.articles  = jsonDatas
    self.outputPath = outputPath

  def parse(self, singleJson):

    if 'abstractText' in singleJson:
      abstract = singleJson['abstractText']
      if abstract == '':
        abstract = '-No abstract-'

    else:
      abstract = '-No abstract-'

    if 'articleTitle' in singleJson:
      title  = singleJson['articleTitle']
      if title == '':
        title = '-No title-'
    else:
      title = '-No title-'

    pmid   = singleJson['pmid']

    return (abstract, title, pmid)

  def clean_line(self, line):

    return ''.join(s for s in line if s in string.printable)

  def save(self):

    with open(self.outputPath, 'w') as fw:
      import sys
      for a in self.articles:
        # parse from json
        ab, ti, pmid = self.parse(a)

        # write to file (3 lines per json)
        line1 = "{0}|t|{1}\n".format(pmid, ti.encode('utf-8'))
        line1 = self.clean_line(line1)
        try:
          line2 = "{0}|a|{1}\n".format(pmid, ab.encode('utf-8'))
          line2 = self.clean_line(line2)
        except UnboundLocalError as err:
          print ab
          print pmid
          print ("UnboundLocal error: {0}".format(err))
          raise
        except UnicodeDecodeError as uniErr:
          print ab
          print pmid
          print ("UnicodeDecode error: {0}".format(uniErr))
          raise
        except:
          print pmid
          print ("Unexpected error:", sys.exc_info()[0])
          raise


        fw.write(line1)
        fw.write(line2)
        fw.write('\n')

class WordExtractor:
  def __init__(self, stepName):
    if stepName == "gene":
      self.extractFunc = self.geneExtract
      #self.geneWordSet = self.getGeneWordSet()
      self.geneDict = self.getGeneDict()
    elif stepName == "chem":
      self.extractFunc = self.chemExtract
    elif stepName == "dis":
      self.extractFunc = self.disExtract
    else:
      raise NameError("Unknown step name!")

  def getExtractFunc(self):
    return self.extractFunc

  def getGeneWordSet(self):
    import wordSet, config
    geneWordSet = wordSet.loadGeneSymbolList(
        config.file_path['gene_symbol_list'])
    return geneWordSet
    
  def getGeneDict(self):
    import wordSet, config
    geneDict = wordSet.loadGeneDict(
        config.file_path['gene_id_symbol_tsv'])
    return geneDict
    
  def geneExtract(self, line):
    factors = line.split('\t')
    wordType = factors[4]
    if wordType != 'Gene':
      return 'NO-PMID', 'NO-WORD'
    pmid = factors[0]
    #wordName = factors[3]
    geneId = factors[5].rstrip()
    if geneId not in self.geneDict:
      return 'NO-PMID', 'NO-WORD'
    else:
      wordName = self.geneDict[geneId]
    return pmid, wordName
    #return "{pmid},{word_name}".format(pmid=pmid, word_name=wordName)

  def chemExtract(self, line):
    factors = line.split('\t')
    wordType = factors[4]
    if wordType != 'Chemical':
      return 'NO-PMID', 'NO-WORD'
    pmid = factors[0]
    wordName = factors[3]
    mesh = factors[5].rstrip()
    return pmid, mesh
    #return "{pmid},{mesh_id},{word_name}".format(pmid=pmid, mesh_id=mesh, word_name=wordName)

  def disExtract(self, line):
    factors = line.split('\t')
    wordType = factors[4]
    if wordType != 'Disease':
      return 'NO-PMID', 'NO-WORD'
    pmid = factors[0]
    wordName = factors[3]
    mesh = factors[5].rstrip()
    return pmid, mesh
    #return "{pmid},{mesh_id},{word_name}".format(pmid=pmid, mesh_id=mesh, word_name=wordName)

class ResultParser(object):

  def __init__(self):
    pass

  def parse(self, line):

    if re.match(r"\d+\t", line):
      status = 'LOADING'
    else:
      status = 'PASS'

    return status

  def getlines(self, input_path):

    table_lines = []

    with open(input_path) as fi:

      for line in fi:
        if self.parse(line) == "LOADING":
          fields = line.rstrip().split("\t")
          if len(fields) == 5:
            fields.append(None)
          table_lines.append(fields)

    return table_lines

class WordSaver(object):

  """
  """
  def __init__(self, inputPath, outputPath, stepName):
    self.inputPath = inputPath
    self.outputPath = outputPath
    self.pmid = 'NO-PMID'
    self.wordSet = set()
    self.wordExtractor = WordExtractor(stepName)
    self.extractFunc = self.wordExtractor.getExtractFunc()

  def parse(self, line):

    if re.match(r"\d+\t", line):
      pmid, word = self.extractFunc(line)
      if pmid == 'NO-PMID':
        status = 'PASS'
      else:
        self.pmid = pmid
        self.wordSet.add(word)
        status = 'LOADING'

    elif re.match(r"^$", line):
      status = 'END'
    else:
      status = 'PASS'

    return status

  def save(self):

    with open(self.inputPath) as fi, open(self.outputPath, 'w') as fw:
      for line in fi:
        status = self.parse(line)

        if status == 'LOADING':
          """ Add word to set """
          pass
        elif status == 'END' and self.pmid != 'NO-PMID':
          for word in self.wordSet:
            outLine = "{pmid},{word}".format(
              pmid=self.pmid, word=word)
            fw.write(outLine)
            fw.write('\n')
          self.wordSet.clear()
        else:
          """ Nothing meaningful """
          pass

class Body(object):

  def __init__(self, bodyString, step):
    self.bodyString = bodyString
    self.step = step

  def genPmidList(self):
    self.pmidList = ast.literal_eval(self.bodyString)

  def getPmidSize(self):
    return len(self.pmidList)

  def chunks(self, l, n):
    for i in range(0, len(l), n):
      yield str(l[i:i + n])

  def chunkPmids(self):
    return(self.chunks(self.pmidList, self.step))

class XmlTagRemover(object):

  def __init__(self):
    pass

  def trim(self, xml_text):
    head_exp = r"<AbstractText Label=.*?>"
    tail_exp = r"</AbstractText>"

    xml_text = re.sub(head_exp, "", xml_text)
    xml_text = re.sub(tail_exp, "", xml_text)

    return xml_text

def main():
  pmidStr = "[1,2,3,4,5]"
  msgBody = Body(pmidStr, 3)
  msgBody.genPmidList()
  for i in msgBody.chunkPmids():
    print i
    print type(i)

if __name__ == "__main__":
  main()

