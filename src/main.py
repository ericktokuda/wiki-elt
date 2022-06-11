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
from myutils import info, create_readme

from downloader import download_dumps
from processor import process_dumps
import bz2
import subprocess

##########################################################
def main(dumpdir, nprocs, outdir):
    """Short description"""
    info(inspect.stack()[0][3] + '()')

    olinksdir = pjoin(outdir, 'links') # Out links
    # os.makedirs(olinksdir, exist_ok=True)

    # download_dumps(lang, date, dumpsdir)
    process_dumps(dumpdir, nprocs, olinksdir)


##########################################################
if __name__ == "__main__":
    info(datetime.date.today())
    t0 = time.time()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dumpdir', default='data/dumps/', help='Dumps directory')
    parser.add_argument('--nprocs', default=1, type=int, help='Dumps directory')
    parser.add_argument('--outdir', default='/tmp/out/', help='Output directory')
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    readmepath = create_readme(sys.argv, args.outdir)

    main(args.dumpdir, args.nprocs, args.outdir)

    info('Elapsed time:{:.02f}s'.format(time.time()-t0))
    info('Output generated in {}'.format(args.outdir))
