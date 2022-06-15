#!/usr/bin/env python3
"""Parse pagelinks wiki dump
"""

import argparse
import time, datetime
import os
from os.path import join as pjoin
from os.path import basename
import inspect

import sys
import numpy as np
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
from myutils import info, create_readme
import pandas as pd
from subprocess import Popen, PIPE, DEVNULL
from mysqltocsv import mysql2csv
import csv

##########################################################
def sqlgz_to_csv(dumppath, outpath):
    """Open compressed (gz) dump, parse its content, and output to csv
    """
    info(inspect.stack()[0][3] + '()')

    stdout = Popen(['zcat'], stdin=open(dumppath),
                             stdout=PIPE).stdout

    # DOING THIS WAY IT IS GENERATING WEIRD CHARACTERS (MAYBE RELATED TO ENCODING)
    if os.path.isfile(outpath):
        info('FILE {} ALREADY EXISTS!\nABORTING...'.format(outpath))
        return

    fh = open(outpath, 'w')

    for l in stdout:
        line = str(l)
        if not 'INSERT INTO' in line: continue
        values = line.split('),(')
        idx = values[0].find('(')
        first = values[0][idx + 1:]
        values[0] = first
        last = values[-1].split(');')[0].strip()
        values[-1] = last

        for v in values:
            print(v, file=fh)

    fh.close()

##########################################################
def sql_to_csv(sqlpath, outpath):
    """Open compressed (gz) dump, parse its content, and output to csv
    """
    info(inspect.stack()[0][3] + '()')

    if os.path.isfile(outpath):
        info('FILE {} ALREADY EXISTS!\nABORTING...'.format(outpath))
        return

    sqlfh = open(sqlpath, 'r')
    csvfh = open(outpath, 'w')

    for l in sqlfh:
        line = str(l)
        if not 'INSERT INTO' in line: continue
        values = line.split('),(')
        idx = values[0].find('(')
        first = values[0][idx + 1:]
        values[0] = first
        last = values[-1].split(');')[0].strip()
        values[-1] = last

        for v in values:
            print(v, file=csvfh)

    csvfh.close()
    sqlfh.close()

##########################################################
def parse_csv_pagelinks(csvpath, outpath):
    """Parse csv file and output to @outpath.
    Table Pagelinks contains 4 fields: srcid, tgtns, tgttitle, srcns
    """
    info(inspect.stack()[0][3] + '()')

    if os.path.isfile(outpath):
        info('FILE {} ALREADY EXISTS!\nABORTING...'.format(outpath))
        return

    fh = open(csvpath)

    rows = []
    for i, l in enumerate(fh):
        els = l.split(',')
        middleels = els[2:-1]

        nssrc = els[-1].strip()
        nstgt = els[1]

        if (not nssrc == '0') or (not nstgt == '0'): continue

        aux1 = []
        for el in middleels:
            aux2 = el
            for c in u'\\"\'`‘’\ufeff':
                aux2 = aux2.replace(c, '')
            aux1.append(aux2.strip())

        middlestr = ','.join(aux1)
        row = [els[0], middlestr]
        rows.append(row)

    df = pd.DataFrame(rows, columns=['srcid', 'tgttitle'])
    df.to_csv(outpath, sep='\t', index=False)
    fh.close()


##########################################################
def parse_csv_page(csvpath, outpath):
    """Parse csv file and output to @outpath.
    Table Pagelinks contains 4 fields: srcid, tgtns, tgttitle, srcns
    """
    info(inspect.stack()[0][3] + '()')

    if os.path.isfile(outpath):
        info('FILE {} ALREADY EXISTS!\nABORTING...'.format(outpath))
        return

    fh = open(csvpath)

    rows = []
    for i, l in enumerate(fh):
        els = l.split(',')
        titleels = els[2:-10]
        pid, ns = els[0], els[1]

        if not ns == '0': continue

        isredir, isnew, latest = els[-9], els[-8], els[-4]
        aux1 = []
        for el in titleels:
            aux2 = el
            for c in u'\\"\'`‘’\ufeff':
                aux2 = aux2.replace(c, '')
            aux1.append(aux2)

        titlestr = ','.join(aux1)
        row = [pid, titlestr, isredir, latest]
        rows.append(row)

    df = pd.DataFrame(rows, columns=['pid', 'title', 'isredir', 'latrev'])
    df.to_csv(outpath, sep='\t', index=False, float_format='%.0f')
    fh.close()

##########################################################
def get_adj_ids(linkstsv, pagetsv, adjidspath, titleerrspath):
    """Short description """
    info(inspect.stack()[0][3] + '()')
    linkdf = pd.read_csv(linkstsv, sep='\t')
    pagedf = pd.read_csv(pagetsv, sep='\t')

    joineddf = linkdf.set_index('tgttitle').join(pagedf.set_index('title'), how='left')
    info(joineddf.count())

    joineddf = joineddf.reset_index()
    joineddf.rename(columns={'index':'tgttitle', 'pid':'tgtid'}, inplace=True)
    joineddf.loc[joineddf.tgtid.notnull()]. \
        to_csv(adjidspath, columns=['srcid', 'tgtid'], index=False, sep='\t')

    joineddf.loc[joineddf.tgtid.isnull()]. \
        to_csv(titleerrspath, columns=['tgttitle'], header=['title'], index=False, sep='\t')

##########################################################
def get_adj_titles(linkstsv, pagetsv, adtitlespath, iderrspath):
    """Short description """
    info(inspect.stack()[0][3] + '()')
    linkdf = pd.read_csv(linkstsv, sep='\t')
    pagedf = pd.read_csv(pagetsv, sep='\t')

    joineddf = linkdf.set_index('srcid').join(pagedf.set_index('pid'), how='left')
    info(joineddf.count())

    joineddf.rename(columns={'title':'srctitle'}, inplace=True)
    joineddf.loc[joineddf.tgtid.notnull()]. \
        to_csv(adjtitlespath, columns=['srctitle', 'tgttitle'], index=False, sep='\t')

    joineddf.loc[joineddf.tgttitle.isnull()]. \
        to_csv(iderrspath, columns=['tgttitle'], header=['title'], index=False, sep='\t')
    joineddf.loc[joineddf.tgttitle.isnull()]. \
        to_csv(iderrspath, columns=[], sep='\t', index=True, index_label='srcid')

##########################################################
if __name__ == "__main__":
    info(datetime.date.today())
    t0 = time.time()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sqldir', help='Path to the uncompressed mysql file.')
    parser.add_argument('--date', default='20220501',
                        help='Date of the dump')
    parser.add_argument('--lang', default='en',
                        help='Wikipedia language')
    parser.add_argument('--outdir', default='/tmp/out/', help='Output directory')
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    readmepath = create_readme(sys.argv, args.outdir)

    lan = args.lang
    suff = '{}wiki-{}-'.format(lan, args.date)
    linkssql = pjoin(args.sqldir, '{}pagelinks.sql'.format(suff))
    pagesql = pjoin(args.sqldir, '{}page.sql'.format(suff))

    linkscsv = pjoin(args.outdir, '{}pagelinks.csv'.format(suff))
    sql_to_csv(linkssql, linkscsv)
    linkstsv = linkscsv.replace('.csv', '.tsv')
    parse_csv_pagelinks(linkscsv, linkstsv)

    pagecsv = pjoin(args.outdir, '{}page.csv'.format(suff))
    sql_to_csv(pagesql, pagecsv)
    pagetsv = pagecsv.replace('.csv', '.tsv')
    parse_csv_page(pagecsv, pagetsv)

    adjidspath = pjoin(args.outdir, '{}adjids.csv'.format(suff))
    titleerrspath = pjoin(args.outdir, '{}titleerrs.csv'.format(suff))
    # get_adj_ids(linkstsv, pagetsv, adjidspath, titleerrspath)
    adjtitlespath = pjoin(args.outdir, '{}adjtitles.csv'.format(suff))
    iderrspath = pjoin(args.outdir, '{}iderrs.csv'.format(suff))
    get_adj_titles(linkstsv, pagetsv, adjtitlespath, iderrspath)

    info('Elapsed time:{:.02f}s'.format(time.time()-t0))
    info('Output generated in {}'.format(args.outdir))
