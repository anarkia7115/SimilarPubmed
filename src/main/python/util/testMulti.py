#!/usr/bin/env python
# -*- coding: utf-8 -*-

from multiprocessing import Pool
import time

def f(i):
    print "ready to sleep {} secs".format(i)
    time.sleep(i)
    print "slept {} secs".format(i)

def main():
    p = Pool(2)
    for i in range(10):
        result = p.apply_async(f, (i, ))

    result.get()

main()
