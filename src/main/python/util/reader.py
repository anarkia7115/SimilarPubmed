"""
  read from raw document
  FORMAT: <doc_id> <TAB> <title> <TAB> <raw_text>

  Parse doc line
    split into (doc_id, title, raw_text)

  @INPUT:  file_name
  @OUTPUT: [(doc_id, title, document), ...]
"""
def read_raw(file_name):
  raw_doc_list = []
  with open(file_name) as f:
    for l in f:
      # check line fields
      if len(l.split("\t")) != 3:
        raise ValueError("fields number of doc "
                         + "line is not 3! [ "
                         + l + " ]")

      # split
      (doc_id, title, raw_text) = l.split("\t")

      # append
      raw_doc_list.append((doc_id, title, raw_text))

  return raw_doc_list

def read_doc(file_name):
  doc_tw = []
  with open(file_name) as f:
    for l in f:
      line_fields = l.rstrip().split("\t")
      pmid = int(line_fields[0])

      tws = line_fields[1:]
      tw_list = []

      for tw in tws:
        tw_fields = tw.split(",")

        term = int(tw_fields[0])
        weight = float(tw_fields[1])

        tw_list.append((term, weight),)

      doc_tw.append((pmid, tw_list),)

  return doc_tw

def read_mat2(file_name):
  term_p = dict()
  term_w = dict()
  lnr = 0
  with open(file_name) as f:
    for l in f:
      line_split = l.rstrip().split("\t\t")
      term_id = int(line_split[0])

      pws = line_split[1:]
      term_p[term_id] = []
      term_w[term_id] = []
      for pw in pws:
        (pmid, weight) = read_pw(pw)
        term_p[term_id].append(pmid)
        term_w[term_id].append(pws)

      if (lnr % 10000 == 0):
        print "{} lines read. ".format(lnr)
      lnr += 1
  return (term_p, term_w)

def read_mat(file_name):
  term_pw = dict()
  lnr = 0
  with open(file_name) as f:
    for l in f:
      line_split = l.rstrip().split("\t\t")
      term_id = int(line_split[0])

      pws = line_split[1:]
      term_pw[term_id] = pws

      if (lnr % 10000 == 0):
        print "{} lines read. ".format(lnr)
      lnr += 1
  return term_pw

def read_pw(pw):
  pw_fields = pw.split(",")
  pmid = int(pw_fields[0])
  weight = float(pw_fields[1])
  return (pmid, weight)

