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
import subprocess

MYSQL2CSV = 'src/mysqlgz2csv.sh'

##########################################################
def sqlgz_to_csv(dumppath, outpath):
    """Open compressed (gz) dump, parse its content, and output to csv
    """
    info(inspect.stack()[0][3] + '()')

    stdout = Popen(['zcat'], stdin=open(dumppath),
                             stdout=PIPE).stdout

    # DOING THIS WAY IT IS GENERATING WEIRD CHARACTERS (MAYBE RELATED TO ENCODING)
    if os.path.isfile(outpath):
        info('FILE {} ALREADY EXISTS! ABORTING...'.format(outpath))
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
def clean_text(txt, mostlyorig=True):
    """Process the title coming from the sql file. If @mostlyorig, just the
    surrounding quotes and space are removed. Else most of the uncommon
    characters are removed."""
    if mostlyorig:
        return txt.strip(" '")
    else:
        for c in u'\\"\'`‘’\ufeff':
            txt = txt.replace(c, '')
        return txt.strip()

##########################################################
def parse_csv_pagelinks(csvpath, outpath):
    """Parse csv file and output to @outpath.
    Table Pagelinks contains 4 fields: srcid, tgtns, tgttitle, srcns
    """
    info(inspect.stack()[0][3] + '()')

    if os.path.isfile(outpath):
        info('FILE {} ALREADY EXISTS! ABORTING...'.format(outpath))
        return

    fh = open(csvpath)

    rows = []
    for i, l in enumerate(fh):
        els = l.split(',')
        titleels = els[2:-1]

        nssrc = els[-1].strip()
        nstgt = els[1]

        if (not nssrc == '0') or (not nstgt == '0'): continue

        title = clean_text(','.join(titleels), mostlyorig=True)

        row = [els[0], title]
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
        info('FILE {} ALREADY EXISTS! ABORTING...'.format(outpath))
        return

    fh = open(csvpath)

    rows = []
    for i, l in enumerate(fh):
        els = l.split(',')
        titleels = els[2:-10]
        pid, ns = els[0], els[1]

        if not ns == '0': continue

        isredir, isnew, latest = els[-9], els[-8], els[-4]
        title = clean_text(','.join(titleels), mostlyorig=True)
        row = [pid, title, isredir, latest]
        rows.append(row)

    df = pd.DataFrame(rows, columns=['pid', 'title', 'isredir', 'latrev'])
    df.to_csv(outpath, sep='\t', index=False, float_format='%.0f')
    fh.close()

##########################################################
def parse_csv_categ(csvpath, outpath):
    """Parse csv file and output to @outpath.
    Table Category contains 6 fields:
    cat_id, cat_title, cat_pages, cat_subcats, cat_files, cat_hidden
    Here we are just interested in cat_id, cat_title, and cat_files
    """
    info(inspect.stack()[0][3] + '()')

    if os.path.isfile(outpath):
        info('FILE {} ALREADY EXISTS! ABORTING...'.format(outpath))
        return

    fh = open(csvpath)

    rows = []
    for i, l in enumerate(fh):
        els = l.split(",")
        pid = els[0]
        n = els[-3]
        titleels = els[1:-3]
        cattitle = clean_text(','.join(titleels), mostlyorig=True)
        row = [pid, cattitle, n]
        rows.append(row)

    df = pd.DataFrame(rows, columns=['pid', 'cattitle', 'npages'])
    df.to_csv(outpath, sep='\t', index=False, float_format='%.0f')
    fh.close()

##########################################################
def parse_csv_categlinks(csvpath, outpath):
    """Parse csv file and output to @outpath.
    Table CategoryLinks contains 6 fields:
    cl_from, cl_to, cl_sortkey, cl_timestamp, cl_sortkey_prefix, cl_collation, cl_type
    Here we are just interested in cl_from (page id), cl_to (categ title) and
    possibly cl_type (page, file, subcat)
    """
    info(inspect.stack()[0][3] + '()')

    if os.path.isfile(outpath):
        info('FILE {} ALREADY EXISTS! ABORTING...'.format(outpath))
        return

    fh = open(csvpath)

    rows = []
    for i, l in enumerate(fh):
        els = l.split("','")
        pid, title = els[0].split(",'")
        # titleels = els[1:-5]
        ptype = els[-1].strip(" '\n")

        if len(els) > 6: breakpoint()
        ispage = 1 if ptype == 'page' else 0

        title = clean_text(title, mostlyorig=True)
        row = [pid, title, ispage]
        rows.append(row)

    df = pd.DataFrame(rows, columns=['pid', 'title', 'ispage'])
    df.to_csv(outpath, sep='\t', index=False, float_format='%.0f')
    fh.close()

##########################################################
def get_adj_ids(linkstsv, pagetsv, adjidspath, titleerrspath):
    """Get adjacency list using ids"""
    info(inspect.stack()[0][3] + '()')

    if os.path.isfile(adjidspath):
        info('FILE {} ALREADY EXISTS! ABORTING...'.format(adjidspath))
        return

    linkdf = pd.read_csv(linkstsv, sep='\t')
    pagedf = pd.read_csv(pagetsv, sep='\t')

    joineddf = linkdf.set_index('tgttitle').join(pagedf.set_index('title'), how='left')

    joineddf = joineddf.reset_index()
    joineddf.rename(columns={'index':'tgttitle', 'pid':'tgtid'}, inplace=True)
    joineddf.loc[joineddf.tgtid.notnull()]. \
        to_csv(adjidspath, columns=['srcid', 'tgtid'], index=False,
               sep='\t', float_format='%.0f')

    errtitles = joineddf.loc[joineddf.tgtid.isnull()].tgttitle
    errtitles = errtitles.to_numpy(dtype='str')
    info('Titles with missing (or invalid) ids: {}'.format(len(errtitles)))
    open(titleerrspath, 'w').write('\n'.join(errtitles))

##########################################################
def get_adj_titles(linkstsv, pagetsv, adtitlespath, iderrspath):
    """Get adjacency list using titles"""
    info(inspect.stack()[0][3] + '()')

    if os.path.isfile(adjtitlespath):
        info('FILE {} ALREADY EXISTS! ABORTING...'.format(adjtitlespath))
        return

    linkdf = pd.read_csv(linkstsv, sep='\t')
    pagedf = pd.read_csv(pagetsv, sep='\t')

    joineddf = linkdf.set_index('srcid').join(pagedf.set_index('pid'), how='left')

    joineddf.rename(columns={'title':'srctitle'}, inplace=True)
    joineddf.loc[joineddf.tgttitle.notnull()]. \
        to_csv(adjtitlespath, columns=['srctitle', 'tgttitle'], index=False, sep='\t')

    errids = joineddf.loc[joineddf.tgttitle.isnull()].index
    errids = errids.to_numpy(dtype='int').astype(str)
    info('Ids with missing (or invalid) titles: {}'.format(len(errids)))
    open(iderrspath, 'w').write('\n'.join(errids))

##########################################################
def mysqlgz2csv(mysqlgzpath, csvpath):
    cmd = ['bash', MYSQL2CSV, mysqlgzpath, csvpath]
    subprocess.run(cmd)

##########################################################
if __name__ == "__main__":
    info(datetime.date.today())
    t0 = time.time()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sqldir', required=True,
                        help='Path to the uncompressed mysql file.')
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

    funcs = {'page': parse_csv_page, # pageid, pagetitle
             'pagelinks': parse_csv_pagelinks, # pageid, pagelinktitles
             'category': parse_csv_categ, # categoryid, categorytitle
             'categorylinks': parse_csv_categlinks # pageid -> categorytitle
             }

    for k, parser in funcs.items():
        gzpath = pjoin(args.sqldir, '{}{}.sql.gz'.format(suff, k))
        csvpath = pjoin(args.outdir, '{}{}.csv'.format(suff, k))
        tsvpath = pjoin(args.outdir, '{}{}.tsv'.format(suff, k))
        mysqlgz2csv(gzpath, csvpath)
        parser(csvpath, tsvpath)

    linkstsv = pjoin(args.outdir, '{}{}.tsv'.format(suff, 'pagelinks'))
    pagetsv = pjoin(args.outdir, '{}{}.tsv'.format(suff, 'page'))

    # Idenfying page from their ids
    adjidspath = pjoin(args.outdir, '{}adjids.tsv'.format(suff))
    titleerrspath = pjoin(args.outdir, '{}titleerrs.tsv'.format(suff))
    get_adj_ids(linkstsv, pagetsv, adjidspath, titleerrspath)

    # Idenfying page from their title
    adjtitlespath = pjoin(args.outdir, '{}adjtitles.tsv'.format(suff))
    iderrspath = pjoin(args.outdir, '{}iderrs.tsv'.format(suff))
    get_adj_titles(linkstsv, pagetsv, adjtitlespath, iderrspath)

    # We can sort these edge list so it is easier to generate the adj list
    # sort -n FILE

    info('Elapsed time:{:.02f}s'.format(time.time()-t0))
    info('Output generated in {}'.format(args.outdir))
