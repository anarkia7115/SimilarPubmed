#!/usr/bin/env python
# -*- coding: utf-8 -*-

from Bio import Entrez
from multiprocessing import Process
import math

def load_pmids(file_name):
    pmids = []

    with open(file_name) as f:
        for l in f:
            pmids.append(l.strip())

    return pmids

def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in xrange(0, len(l), n))

def save_chunck_pmids(chunks, out_file_prefix):

    i = 0
    for c in chunks:
        with open(chunk_input(out_file_prefix, i), 'w') as fw:
            for pmid in c:
                fw.write(pmid + "\n")

        i += 1

def chunk_input(chunk_prefix, chunk_index):
    return "{}_{}.txt".format(chunk_prefix, chunk_index)

def proc_out(proc_prefix, proc_index):
    return "{}_{}.txt".format(proc_prefix, proc_index)

def load_chunk_input(chunk_prefix, chunk_index):
    pmids_path = chunk_input(chunk_prefix, chunk_index)
    chunk = load_pmids(pmids_path)
    return chunk

def main():
    file_name = "/home/shawn/data/SimilarPubmed/pmid_range_may10.txt"
    prev_file = "/home/shawn/data/SimilarPubmed/prev_pmid.txt"

    all_pmids = load_pmids(file_name)
    prev_pmids = set(load_pmids(prev_file))
    pmids = list(set(all_pmids).difference(prev_pmids))
    #print "pmids: {}".format(",".join(pmids))
    #print "pmids size: ".format(len(pmids))

    #thread_num = 100
    #chunk_size = int(math.ceil(len(pmids) * 1.0 / thread_num))
    chunk_size = 10
    chunk_pmids =  chunks(pmids, chunk_size)

    proc_prefix = "/home/shawn/data/SimilarPubmed/pubmed_neighour_proc/pubmed_neighour_proc"
    chunk_prefix = "/home/shawn/data/SimilarPubmed/pmid_chunks/pmid_chunk"
    save_chunck_pmids(chunk_pmids, chunk_prefix)

    # Generate process list
    proc_list = []
    for proc_index in range(2):
        p = Process(target=process_elink, args=(
            proc_prefix, chunk_prefix, proc_index))
        proc_list.append(p)
        p.start()

    # Timeout Join
    timeout_sec = 350
    for p in proc_list:
        p.join(timeout_sec)

def process_elink(process_prefix, chunk_prefix, process_index):
    out_file = proc_out(process_prefix, process_index)
    pmids = load_chunk_input(chunk_prefix, process_index)
    prev_out_file = "/home/shawn/data/SimilarPubmed/prev_pmid.txt".format(process_index)
    with open(out_file, "w") as fw, open(prev_out_file, "w") as fpw:
        for src_pmid in pmids:
            print "{} is adding in process {}...".format(src_pmid,
                                                         process_index)
            records = neighbor_records(src_pmid)
            result_lines = analyze_records(src_pmid, records)
            for l in result_lines:
                fw.write(l)
            #fpw.write("{}\n".format(src_pmid))
    
def neighbor_records(pmids):
    Entrez.email = "shawn24601@gmail.com"
    records = Entrez.read(Entrez.elink(dbfrom="pubmed", db="pubmed", id=pmids, cmd="neighbor_score"))
    return records

def analyze_records(src_pmid, records):
    # dummy
    record = records[0]
    result_lines = []

    for link_set in record['LinkSetDb']:
        link_name = link_set['LinkName']
        links = link_set['Link']
        for l in links:
            score = l['Score']
            rel_pmid = l['Id']
            # src_pmid, rel_pmid, score, link_name
            data_line = "{}\t{}\t{}\t{}\n".format(
                src_pmid, rel_pmid, score, link_name)
            result_lines.append(data_line)

    return result_lines

if __name__ == "__main__":
    main()

