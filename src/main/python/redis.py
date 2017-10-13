#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ast
import redis
import util.reader
import util.put
import operator
import time

def main():
    mat_one_dict()

def mat_put_redis():
    td_file = "/home/gcbi/data/termDocTable.txt"
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    util.put.to_redis_h(r, td_file)

def mat_redis():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    dt_file = "/home/gcbi/data/docTermInRange.txt"
    #td_file = "/home/gcbi/data/termDocTable.txt"
    dts = util.reader.read_doc(dt_file)
    #td_dict = util.reader.read_mat(td_file)
    print "Read Done!"
    file_out = "/home/gcbi/data/result_1w.csv"

    pmid_nr = 0

    # write result
    with open(file_out, 'w') as fw:

        # read from doc
        for dt in dts:
            src_pmid = dt[0]
            tws = dt[1]
            score_dict = dict()

            # read from term-weight
            for tw in tws:
                term = tw[0]
                src_weight = tw[1]

                # get pmid-weight from mat
                #pws = td_dict.get(term, [])
                pws = r.get("term_id:{}".format(term))
                if pws is None:
                    pws = dict()

                # add scores
                for (rel_pmid, rel_weight) in pws:
                    score_dict[rel_pmid] = src_weight * rel_weight + score_dict.get(rel_pmid, 0.0)


            # sort by score
            sorted_scores = sorted(score_dict.items(), key=operator.itemgetter(1))
            top_scores = sorted_scores[-101:]

            # write to file
            print "{}:{} write to file".format(pmid_nr, src_pmid)
            for (rel_pmid, weight) in top_scores:
                fw.write("{}\t{}\t{}\n".format(src_pmid, rel_pmid, weight))

            pmid_nr += 1



def mat_2_dict():
    dt_file = "/home/gcbi/data/docTermInRange.txt"
    td_file = "/home/gcbi/data/termDocTable.txt"
    dts = util.reader.read_doc(dt_file)
    (p_dict, w_dict) = util.reader.read_mat2(td_file)
    print "Read Done!"
    file_out = "/home/gcbi/data/result_1w.csv"

    pmid_nr = 0

    # write result
    with open(file_out, 'w') as fw:

        # read from doc
        for dt in dts:
            src_pmid = dt[0]
            tws = dt[1]
            score_dict = dict()

            # read from term-weight
            for tw in tws:
                term = tw[0]
                src_weight = tw[1]

                # get pmid-weight from mat
                rel_pmids = p_dict.get(term, [])
                rel_weights = w_dict.get(term, [])
                for i in range(len(rel_pmids)):
                    rel_pmid = rel_pmids[i]
                    rel_weight = rel_weights[i]
                    score_dict[rel_pmid] = src_weight * rel_weight + score_dict.get(rel_pmid, 0.0)

            # sort by score
            sorted_scores = sorted(score_dict.items(), key=operator.itemgetter(1))
            top_scores = sorted_scores[-101:]

            # write to file
            print "{}:{} write to file".format(pmid_nr, src_pmid)
            for (rel_pmid, weight) in top_scores:
                fw.write("{}\t{}\t{}\n".format(src_pmid, rel_pmid, weight))

            pmid_nr += 1

def mat_one_dict():
    dt_file = "/home/gcbi/data/docTermInRange.txt"
    td_file = "/home/gcbi/data/termDocTable.txt"
    dts = util.reader.read_doc(dt_file)
    td_dict = util.reader.read_mat(td_file)
    print "Read Done!"
    print "using one dict in mem!"
    file_out = "/home/gcbi/data/result_1w.csv"

    pmid_nr = 0

    # write result
    with open(file_out, 'w') as fw:

        # read from doc
        for dt in dts:
            src_pmid = dt[0]
            tws = dt[1]
            score_dict = dict()

            #rel_pw_dict_list = get_related_pw(tws, td_dict)

            merged_dict = get_related_pw_use_dict(tws, td_dict)
            #merged_dict = merge_pw_list(rel_pw_dict_list)

            top_scores = sort_scores(merged_dict, 101)
            # write to file
            print "{}:{} write to file".format(pmid_nr, src_pmid)
            for (rel_pmid, weight) in top_scores:
                fw.write("{}\t{}\t{}\n".format(src_pmid, rel_pmid, weight))

            pmid_nr += 1

def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print '%s function took %0.3f ms' % (f.func_name, (time2-time1)*1000.0)
        return ret
    return wrap

@timing
def sort_scores(merged_dict, top_k):
    # sort by score
    sorted_scores = sorted(merged_dict.items(), key=operator.itemgetter(1),
                           reverse=True)
    top_scores = sorted_scores[:top_k]
    return top_scores

@timing
def merge_pw_list(rel_pw_dict_list):
    print "start reducing..."

    merged_dict = reduce(dict_merge, rel_pw_dict_list)
    return merged_dict

@timing
def get_related_pw_use_dict(tws, td_dict):

    relpmid_score_dict = dict()
    # read from term-weight
    for tw in tws:
        # ignore term
        term = tw[0]
        src_weight = tw[1]

        # get pmid-weight from mat
        pws = td_dict.get(term, [])

        rel_pw_dict = dict()
        # add scores
        for pw in pws:
            (rel_pmid, rel_weight) = util.reader.read_pw(pw)
            relpmid_score_dict[rel_pmid] = src_weight * rel_weight + \
                    relpmid_score_dict.get(rel_pmid, 0.0)
            pass

    return relpmid_score_dict

@timing
def get_related_pw_use_reduce(tws, td_dict):

    rel_pw_dict_list = []
    # read from term-weight
    for tw in tws:
        # ignore term
        term = tw[0]
        src_weight = tw[1]

        # get pmid-weight from mat
        pws = td_dict.get(term, [])

        rel_pw_dict = dict()
        # add scores
        for pw in pws:
            (rel_pmid, rel_weight) = util.reader.read_pw(pw)
            rel_pw_dict[rel_pmid] = src_weight * rel_weight
            pass

        rel_pw_dict_list.append(rel_pw_dict)

    return rel_pw_dict_list

def dict_merge(d1_src, d2):
    d1 = d1_src.copy()
    for k, v in d2.iteritems():
        if (k in d1):
            d1[k] += d2[k]
        else:
            d1[k] = d2[k]
    return d1


if __name__ == "__main__":
    main()
