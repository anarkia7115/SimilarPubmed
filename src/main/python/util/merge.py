#!/usr/bin/env python
# -*- coding: utf-8 -*-

def load_lines(input_file):
    with open(input_file) as f:
        lines = f.readlines()

    return lines

def distinct_pmids(lines):
    pmid_set = set()
    for l in lines:
        pmid_set.add(get_pmid(l))

    return pmid_set

def get_pmid(line):
    return line.split("\t")[0]

def merge(fn1, fn2):
    result_lines = []
    l1 = load_lines(fn1)
    l2 = load_lines(fn2)
    l1_set = distinct_pmids(l1)
    print len(l1_set)
    result_lines += l1
    for l in l2:
        if get_pmid(l) not in l1_set:
            result_lines.append(l)

    return result_lines

def main():
    input_file1 = "/home/shawn/data/SimilarPubmed/result/pubmed_neighour.txt"
    input_file2 = "/home/shawn/data/SimilarPubmed/result/pubmed_neighour2.txt"
    output_file = "/home/shawn/data/SimilarPubmed/result/pubmed_neighbor_all.txt"

    with open(output_file, "w") as fw:
        fw.writelines(merge(input_file1, input_file2))

main()
