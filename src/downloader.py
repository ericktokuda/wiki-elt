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

import bz2
import subprocess

##########################################################
def download_dumps(lang, date):
    """Short description """
    info(inspect.stack()[0][3] + '()')
    outdir = './outXXX/'
    base_url = 'https://dumps.wikimedia.org/enwiki/'
    index = requests.get(base_url).text
    soup_index = BeautifulSoup(index, 'html.parser')

    dumps = [a['href'] for a in soup_index.find_all('a') if a.has_attr('href')]

    dump_url = os.path.join(base_url, '20220501/')
    print(dump_url)


    dump_html = requests.get(dump_url).text
    print(dump_html[:10])

    soup_dump = BeautifulSoup(dump_html, 'html.parser')
    soup_dump.find_all('li', {'class': 'file'}, limit = 10)[:4]

    files = []

    for file in soup_dump.find_all('li', {'class': 'file'}):
        text = file.text
        if 'pages-articles' in text:
            files.append((text.split()[0], text.split()[1:]))
    print(files[:5])

    files_to_download = [file[0] for file in files if '.xml-p' in file[0]]
    print(files_to_download[-5:])


    data_paths = []
    file_info = []


    for file in files_to_download:
        filepath = os.path.join(outdir, file)
        data_paths.append(filepath)

        if not os.path.exists(filepath):
            print('Downloading')
            data_paths.append(filepath)
            r = requests.get(os.path.join(dump_url, file), allow_redirects=True)
            open(dump_url, 'wb').write(r.content)

        file_size = os.stat(filepath).st_size / 1e6
        file_articles = int(file.split('p')[-1].split('.')[-2]) - int(file.split('p')[-2])
        file_info.append((file, file_size, file_articles))

    aux = sorted(file_info, key = lambda x: x[1], reverse = True)[:5]
    print(aux)
    breakpoint()

    sorted(file_info, key = lambda x: x[2], reverse = True)[:5]
    print(f'There are {len(file_info)} partitions.')

    import pandas as pd
    import matplotlib.pyplot as plt
    get_ipython().run_line_magic('matplotlib', 'inline')
    file_df = pd.DataFrame(file_info, columns = ['file', 'size (MB)', 'articles']).set_index('file')
    file_df['size (MB)'].plot.bar(color = 'red', figsize = (12, 6));

    print(f"The total size of files on disk is {file_df['size (MB)'].sum() / 1e3} GB")
    data_path = data_paths[15]
    # data_path

