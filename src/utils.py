#!/usr/bin/env python3
"""Parse pagelinks wiki dump
"""

import argparse
import time, datetime
import os
from os.path import join as pjoin
import inspect

import sys
import numpy as np
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
from myutils import info, create_readme
import pandas as pd
from subprocess import Popen, DEVNULL

##########################################################
def sql2tsv(gzpath, outdir):
    """Convert compressed (.gz) mysql code to tsv (tab delimited values)"""
    info(inspect.stack()[0][3] + '()')

    tmpfile1 = pjoin(outdir, 'temp1.csv')
    tmpfile2 = pjoin(outdir, 'temp2.csv')

    cmd = '''zcat "{}" | grep $'^INSERT INTO ' | sed 's/),(/\n/g' > "{}"'''. \
        format(gzpath, tmpfile1)
    print(cmd)
    p = Popen(cmd, shell=True, stdout=DEVNULL, stderr=DEVNULL)
    p.communicate()

    fh = open(tmpfile1)
    firstline = fh.readline()
    breakpoint()
    
    #TODO: process first line
    remaining = fh.read()

    open(tmpfile2, 'w').write(firstline + remaining)

    # cmd = '''sed 's/.*://' "{}"'''.format(firstline)
    # Popen(cmd, shell=True, stdout=DEVNULL, stderr=DEVNULL)

    # csvpath = '/scratch/tokuda/wiki/dumps/enwiki20220501/enwiki-20220501-pagelinks.sql'
    # csvpath = '/scratch/tokuda/gawiki_pagelinks_sql.csv'
    csvpath = '/scratch/tokuda/enwiki_pagelinks_sql.csv'
    fh = open(csvpath)

    # Pagelinks contains 4 fields:
    # srcid, tgtns, tgttitle, srcns
    rows = []
    for i, l in enumerate(fh):
        # if i > 3: break
        els = l.split(',')
        middleels = els[2:-1]
        if (not els[-1].strip() == '0') or (not els[1] == '0'):
            continue

        aux1 = []
        for el in middleels:
            # aux2 = el.replace('\\', '').strip(''''"''')
            aux2 = el
            for c in '\\"\'`‘’':
                aux2 = aux2.replace(c, '')
            # aux2 = aux2.replace(c, '') for c in '\\"\''
            aux1.append(aux2)

        middlestr = ','.join(aux1)
        row = [els[0], els[1], middlestr, els[-1].strip()]
        rows.append(row)

    pd.DataFrame(rows).to_csv('/scratch/tokuda/foo.tsv', sep='\t', header=False, index=False)

    fh.close()

    info('For Aiur!')

##########################################################
def sql2tsv_old(gzpath, outdir):
    """Convert compressed (.gz) mysql code to tsv (tab delimited values)"""
    info(inspect.stack()[0][3] + '()')

    csvpath = '/scratch/tokuda/enwiki_pagelinks_sql.csv'
    fh = open(csvpath)

    # Pagelinks contains 4 fields:
    # srcid, tgtns, tgttitle, srcns
    rows = []
    for i, l in enumerate(fh):
        # if i > 3: break
        els = l.split(',')
        middleels = els[2:-1]
        if (not els[-1].strip() == '0') or (not els[1] == '0'):
            continue

        aux1 = []
        for el in middleels:
            aux2 = el
            for c in '\\"\'`‘’':
                aux2 = aux2.replace(c, '')
            aux1.append(aux2)

        middlestr = ','.join(aux1)
        row = [els[0], els[1], middlestr, els[-1].strip()]
        rows.append(row)

    pd.DataFrame(rows).to_csv('/scratch/tokuda/foo.tsv', sep='\t', header=False, index=False)
    fh.close()

    info('For Aiur!')

##########################################################
if __name__ == "__main__":
    info(datetime.date.today())
    t0 = time.time()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sqlgz', default='/tmp/out/', help='Path to the comprssed (gunzip) mysql file.')
    parser.add_argument('--outdir', default='/tmp/out/', help='Output directory')
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    readmepath = create_readme(sys.argv, args.outdir)

    sql2tsv_old(args.sqlgz, args.outdir)

    info('Elapsed time:{:.02f}s'.format(time.time()-t0))
    info('Output generated in {}'.format(args.outdir))
