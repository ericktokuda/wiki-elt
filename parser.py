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


##########################################################
def parse_links(dumpsdir, olinksdir):
    """Parse the links from the dumps"""
    info(inspect.stack()[0][3] + '()')

    # f = 'xml.bz2'
    # stdout = subprocess.Popen(['bzcat'], stdin=open(f),
                             # stdout=subprocess.PIPE).stdout

    stdout = 'data/toy02.xml'

    dump = mwxml.Dump.from_file(stdout)

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
            # pass
            print(rev.parent_id, rev.id, rev.text)
            x = mwparse(rev.text)
            print(x.filter_wikilinks())
            print('######################')
            print(x.filter_external_links())
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

    


