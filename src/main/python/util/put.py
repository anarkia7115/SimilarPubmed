import reader

def to_redis_h(r, td_file):

    lnr = 0
    pipe = r.pipeline()
    with open(td_file) as f:
        for l in f:
            line_split = l.rstrip().split("\t\t")
            term_id = int(line_split[0])

            pws = line_split[1:]
            pw_list = []
            for pw in pws:
                (pmid, weight) = reader.read_pw(pw)
                #pw_list.append((pmid, weight),)

                pipe.hset("term_id:{}".format(term_id)
                     , pmid
                     , weight)

            if (lnr % 10000 == 0):
                print "{} lines put to redis...flush ".format(lnr)
                pipe.execute()
            lnr += 1

def to_redis(r, td_file):

    lnr = 0
    pipe = r.pipeline()
    with open(td_file) as f:
        for l in f:
            line_split = l.rstrip().split("\t\t")
            term_id = int(line_split[0])

            pws = line_split[1:]
            pw_list = []
            for pw in pws:
                (pmid, weight) = reader.read_pw(pw)
                pw_list.append((pmid, weight),)

            pipe.set("term_id:{}".format(term_id), pw_list)

            if (lnr % 10000 == 0):
                print "{} lines put to redis...flush ".format(lnr)
                pipe.execute()
            lnr += 1

