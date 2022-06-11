#!/usr/bin/env python3
"""Parse Wikipedia data
Inspired by the Deep Learning Cookbook, from D.Osinga.
"""

import argparse
import time, datetime
import os, sys
from os.path import join as pjoin
import inspect

import numpy as np
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
from myutils import info, create_readme, append_to_file

import bz2
import subprocess
import mwxml, mwparserfromhell
from mwparserfromhell import parse as mwparse
import pandas as pd
import multiprocessing
from multiprocessing import Pool
import re
from subprocess import Popen, DEVNULL
import traceback

SEP = '##########################################################'

class Wikitype:
    BROKEN    = -1
    REGULAR   =  0
    FILE      =  1
    CATEGORY  =  2
    SELF      =  3
    EXTERNAL  =  4
    INTERLANG =  5

# LANG DEPENDANT
# filens = ['Image', 'Media', 'File']
# wikins = ['WP', 'Wikipedia', 'Project']
# talkns = ['WT', 'Project talk', 'Project talk']
# userns = ['User']

# Source: https://en.wikipedia.org/wiki/Wikipedia:Namespace
# Non-talk and non-deprecated namespace ids
ns_ids = [0, 2, 4, 6, 8, 10, 12, 14, 100, 118, 710, 828, -1, -2]
ns_sbj = ['Main', 'Article', 'User', 'Wikipedia', 'File', # Subject
        'MediaWiki' 'Template', 'Help', 'Category', 'Portal',
        'Draft', 'TimedText', 'Module', 'Gadget', 'Gadget definition']
ns_sbja = ['WP']
ns_tk = ['Talk'] + [s + ' talk' for s in ns_sbj[2:]] # Talk
ns_tka  = ['WT']
ns_vrt = ['Special', 'Media'] # Virtual
ns_vrta = ['Image', 'File']
wikins = ns_sbj + ns_sbja + ns_tk + ns_tka + ns_vrt + ns_vrta

redirlowerkw = '#redirect' # Not case sensitive

##########################################################
def classify_wikilinks(links):
    """Classify wiki link"""
    for l in links:
        link = l[2:-2]

        pipeidx = link.find('|') # Remove text of the link
        if pipeidx >= 0: link = link[:pipeidx]

        if link == '': info('Error. Simple "|" found')

        link1 = 'page'
        link2 = 'page#section'
        link3 = ':page#section'
        link4 = 'Help:page#section'

        link = link4


        pipeidx = link.find('#')
        if pipeidx == 0:
            # Link points to a section of the same page
            info(5)
            pass
        else: # Remove section of the link
            link = link[:pipeidx]

        tokens = link.split(':')
        ncommas = len(tokens) - 1

        if ncommas == 0:
            info(10)
            # A regular inner link (eg 'Birds')
        elif ncommas == 1: # Here (n==1) we may still have an inner link
            if tokens[0] == '':
                info(15)
                # Transcluded inner link (eg ':Birds'))
                # We are treating as a regular wikilink
            else:
                # One single ns (eg 'Help:People')
                # Will need to parse the ns
                info(20)
        else:
            # if len(t
            # if tokens[0] in filens:
                # continue
            # elif parts
            # Will need to check 'Image, Wikipedia, Wikibooks,
            pass

        breakpoint()

def simplify_tag(tag, sep):
    return tag.replace('\t', ' ').strip('[]" ').split(sep)[0].strip()
def remove_section(link): return link.split('#')[0]

##########################################################
def process_dump_protected(dumppath, outdir):
    """Exception-protected processing.
    Any unknown error is captured here and output to @outdir/errors.log."""
    try:
        process_dump(dumppath, outdir)
    except Exception as e:
        msg = traceback.format_exc()
        errpath = pjoin(outdir, 'errors.log')
        open(errpath, 'a').write('{}\n{}\n{}\n'.format(dumppath, msg, SEP))

##########################################################
def is_redirect(txt, redirlowerkw):
    return True if re.search('^\s*' + re.escape(redirlowerkw), txt) else False

##########################################################
def process_revision(rev):
    """Process a mwxml revision. Returns the information of the revision, the links,
    and whether the revision the link errors.
    Return:
    revision: [revid, pageid, parentid, timestamp, userid, isminor]
    links: list of list [revid, link, isurl, isredirlink]
    succ: whether the execution was successfull
    """

    parid = rev.parent_id if rev.parent_id else -1
    userid = -1 if ((not rev.user) or (not rev.user.id)) else rev.user.id
    isminor = 1 if rev.minor else 0

    revrow = [rev.id, rev.page.id, parid, rev.timestamp, userid, isminor]

    if rev.text == None: return revrow, [], True # No content

    try:
        aux = mwparse(rev.text)
        wikilinks = aux.filter_wikilinks()
        extlinks = aux.filter_external_links()
    except Exception as e: # Malformed Wikitext. Disregard this content
        return revrow, [], False

    if is_redirect(rev.text, redirlowerkw): # Assume redirect link is not a url
        if len(wikilinks) == 0: return revrow, [], False
        link = remove_section(simplify_tag(wikilinks[0], '|'))
        if link == '':
            return revrow, [], False
        linkrows = [[rev.id, link, 0, 1]]
        return revrow, linkrows, True

    linkrows = []
    for l in wikilinks:
        try:
            link = remove_section(simplify_tag(l, '|'))
            if link == '': link = rev.page.title # Self-loop
            linkrows.append([rev.id, link, 0, 0])
        except Exception as e: #Malformed link
            continue

    for l in extlinks:
        try:
            link = simplify_tag(l, ' ')
            linkrows.append([rev.id, link, 1, 0])
        except Exception as e: #Malformed url
            continue

    return revrow, linkrows, True

##########################################################
def process_dump(dumppath, outdir):
    """Process a dump file. Just accepts historical dumps in bz2 format"""
    fname = os.path.basename(dumppath)

    if (not fname.endswith('.bz2')) or (not 'history' in fname):
        return

    aux = fname.replace('.bz2', '')

    if '.xml-' in aux:
        suff = aux.split('.xml-')[1]
        aux = suff.split('p')
        aux = int(aux[2]) - int(aux[1])
    else:
        suff = 'p0p0'
        aux = 'all'

    info('{} ({} pages)'.format(suff, aux))

    stdout = subprocess.Popen(['bzcat'], stdin=open(dumppath),
                             stdout=subprocess.PIPE).stdout

    pagepath0 = pjoin(outdir, suff + '_pages.tsv')
    revpath0 = pjoin(outdir, suff + '_revs.tsv')
    errpath0 = pjoin(outdir, suff + '_reverr.tsv')
    linkpath0 = pjoin(outdir, suff + '_links.tsv')

    if os.path.isfile(pagepath0): return # Already processed dump

    dump = mwxml.Dump.from_file(stdout)
    #TODO: Parse header to get the namespaces for this language, givne the ns ids

    pageinfos = [] # Store page information

    tmpdir = pjoin(outdir, suff)
    if os.path.isdir(tmpdir):
        info('Continuing previous run {}'.format(suff))
    else:
        os.makedirs(tmpdir)
        info('Started running {}'.format(suff))

    for i, page in enumerate(dump):
        info('Pageid {}'.format(page.id))
        # if i > 3: break # For debugging
        if not page.namespace in ns_ids: continue
        isredir = 1 if page.redirect else 0
        pageinfos.append([page.id, page.title, page.namespace, isredir])

        revpath1 = pjoin(outdir, tmpdir, '{:08d}_revs.tsv'.format(page.id))
        linkpath1 = pjoin(outdir, tmpdir, '{:08d}_links.tsv'.format(page.id))
        errpath1 = pjoin(outdir, tmpdir, '{:08d}_reverr.tsv'.format(page.id))

        if os.path.isfile(revpath1): continue

        revrows = []
        linkrows = []
        errrows = []
        for rev in page:
            revinfo, revlinks, succ = process_revision(rev)
            revrows.append(revinfo)
            linkrows.extend(revlinks)
            if not succ: errrows.append(rev.id)

        pd.DataFrame(revrows).to_csv(revpath1, sep='\t', index=False, header=False)
        pd.DataFrame(linkrows).to_csv(linkpath1, sep='\t', index=False, header=False)
        pd.DataFrame(errrows).to_csv(errpath1, sep='\t', index=False, header=False)

    cmd1 = '''find '{}' -maxdepth 1 -type f -name '*_revs.tsv' -print0 | sort -z | xargs -0 cat -- > '{}' '''.format(tmpdir, revpath0)
    cmd2 = '''find '{}' -maxdepth 1 -type f -name '*_links.tsv' -print0 | sort -z | xargs -0 cat -- > '{}' '''.format(tmpdir, linkpath0)
    cmd3 = '''find '{}' -maxdepth 1 -type f -name '*_reverr.tsv' -print0 | sort -z | xargs -0 cat -- > '{}' '''.format(tmpdir, errpath0)
    cmd4 = ''' rm -rf '{}' '''.format(tmpdir)
    cmd = ' && '.join([cmd1, cmd2, cmd3, cmd4])
    info('cmd:{}'.format(cmd))
    Popen(cmd, shell=True, stdout=DEVNULL, stderr=DEVNULL)
    pd.DataFrame(pageinfos).to_csv(pagepath0, sep='\t', index=False, header=False)
    info('Finished {}'.format(suff))

##########################################################
def parse_link(link):
    """Short description """
        # pages.append
    # stdout = 'data/toy02.xml'
    # langspath = './data/wikilangs.csv'
    # aux = pd.read_csv('./data/wikilangs.csv')
    # aux = aux.loc[aux.available == 'active']['abbr'].tolist()


        # rev = next((x for x in page), None)
        ## I am assuming the revisions are sorted by time (oldest first)
        ## Thus, the first entry is the birth of the page
        # if rev.parent_id != None:
            # info('ERROR: First revision has a parent. ('
                 # 'page.id:{}, rev1:{}, rev1.parentid:{})'.
                 # format(page.id, rev.id, rev.parent_id))

        # We may want to store these rev info: redirect, minor

        # If no <nowiki />, or <nowiki></nowiki> mwxml does not work properly

    if True:
        for rev in page:
            try:
                x = mwparse(rev.text)
            except Exception as e:
                # Malformed Wikitext
                breakpoint()

            print(x.filter_wikilinks())
            wikilinks1 = x.filter_wikilinks()
            extlinks1 = x.filter_external_links()

            print(extlinks1)
            print('######################')
            print(wikilinks1)
            print('######################')
            # wikilinks2 = classify_wikilinks(wikilinks1)
            breakpoint()


    # TODO: Need to generate revpath and linkpath

    # Last thing to be done
    pd.DataFrame(pageinfos).to_csv(pagepath, sep='\t', index=False, header=False)
    return


    breakpoint()
    handler = WikiXmlHandler()
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    stdout = subprocess.Popen(['bzcat'], stdin=open(f),
                             stdout=subprocess.PIPE).stdout

    lines = []
    for i, line in enumerate(stdout):
        parser.feed(line)

        if len(handler._pages) > 10:
            break

    import mwparserfromhell
    wiki = mwparserfromhell.parse(handler._pages[7][1])

##########################################################
def process_dumps(dumpsdir, nprocs, outdir):
    """Parse the links from the dumps.
    We expect a filename in the following format
    # enwiki-DATE-pages-meta-history26.xml-p63665560p63954312.bz2
    """
    info(inspect.stack()[0][3] + '()')

    os.makedirs(outdir, exist_ok=True)

    dumppaths = []
    langs = set(); dumpdts = set(); dumpgrs = set()
    for f in os.listdir(dumpsdir):
        # if (not '-pages-meta-history' in f) or (not f.endswith('.bz2')):
            # continue
        # p1, p2 = f.replace('.bz2', '').split('.xml-')

        # langs.add(p1.split('wiki')[0])
        # dumpdts.add(p1.split('-')[1])
        # dumpgrs.add(int(p1.split('history')[1])) # Dump group (machine in the cluster)
        dumppaths.append(pjoin(dumpsdir, f))

    # if len(langs) > 1 or len(dumpdts) > 1 or len(dumpgrs) < max(dumpgrs):
        # info('Inconsistency in name os the dumps.')
        # info('langs:{}, dumpdts:{}, dumpgrs:{}'.format(langs, dumpdts, dumpgrs))
        # info('Aborting...')
        # return

    dumppaths = sorted(dumppaths)
    # lang = list(langs)[0]; dumpdt = list(dumpdts)[0]

    if nprocs == 1:
        # [ process_dump_protected(p, outdir) for p in dumppaths ]
        [ process_dump(p, outdir) for p in dumppaths ]
    else:
        fargs = [ [p, outdir] for p in dumppaths ]
        with Pool(processes=nprocs) as pool:
            L = pool.starmap(process_dump_protected, fargs)
