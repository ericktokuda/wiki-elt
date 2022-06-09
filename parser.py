#!/usr/bin/env python3
"""Parse Wikipedia data
Inspired by the Deep Learning Cookbook, from D.Osinga.
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
from myutils import info, create_readme, append_to_file

import bz2
import subprocess
import mwxml, mwparserfromhell
from mwparserfromhell import parse as mwparse
import pandas as pd


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

# https://en.wikipedia.org/wiki/Wikipedia:Namespace
ns_sbj = ['Main', 'Article', 'User', 'Wikipedia', 'File', # Subject
        'MediaWiki' 'Template', 'Help', 'Category', 'Portal',
        'Draft', 'TimedText', 'Module', 'Gadget', 'Gadget definition']
ns_sbja = ['WP']
ns_tk = ['Talk'] + [s + ' talk' for s in ns_sbj[2:]] # Talk
ns_tka  = ['WT']
ns_vrt = ['Special', 'Media'] # Virtual
ns_vrta = ['Image', 'File']
wikins = ns_sbj + ns_sbja + ns_tk + ns_tka + ns_vrt + ns_vrta

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
    
##########################################################
def parse_links(dumpsdir, olinksdir):
    """Parse the links from the dumps"""
    info(inspect.stack()[0][3] + '()')

    # f = 'xml.bz2'
    # stdout = subprocess.Popen(['bzcat'], stdin=open(f),
                             # stdout=subprocess.PIPE).stdout

    stdout = 'data/toy02.xml'

    langspath = './data/wikilangs.csv'
    aux = pd.read_csv('./data/wikilangs.csv')
    aux = aux.loc[aux.available == 'active']['abbr'].tolist()

    dump = mwxml.Dump.from_file(stdout)
     #TODO: Parse header to get the namespaces for this language, givne the ns ids

    for page in dump:
        # rev = next((x for x in page), None)
        ## I am assuming the revisions are sorted by time (oldest first)
        ## Thus, the first entry is the birth of the page
        # if rev.parent_id != None:
            # info('ERROR: First revision has a parent. ('
                 # 'page.id:{}, rev1:{}, rev1.parentid:{})'.
                 # format(page.id, rev.id, rev.parent_id))

        # We may want to store these rev info: redirect, minor

        # If no <nowiki />, or <nowiki></nowiki> mwxml does not work properly

        for rev in page:
            # TODO: check REDIRECT
            print(rev.parent_id, rev.id, rev.text)
            x = mwparse(rev.text)
            print(x.filter_wikilinks())
            wikilinks1 = x.filter_wikilinks()
            extlinks1 = x.filter_external_links()

            print(extlinks1)
            print('######################')
            print(wikilinks1)
            print('######################')
            # wikilinks2 = classify_wikilinks(wikilinks1)
            breakpoint()
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

    


