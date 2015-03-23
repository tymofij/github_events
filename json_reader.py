#!/usr/bin/env python
# -*- coding: utf-8 -*-
# requirement: unicodecsv

import json
import unicodecsv as csv
import argparse
import sys

import os
from os import listdir
from os.path import isfile, join
from multiprocessing import Pool

current_path = os.path.dirname(os.path.realpath(__file__))
parser = argparse.ArgumentParser(
    description='Filter WatchEvent and FollowEvent, convert JSON to CSV.')
parser.add_argument('dir_path', type=str, help='Name of JSON file to process')

args = parser.parse_args()
dir_path = join(current_path, vars(args)['dir_path'])

active_users = set()
for l in file('active_users.csv').readlines():
	active_users.add(l.strip().split(',')[0])

popular_repos = set()
for l in file('popular_repos_1000.csv').readlines():
	popular_repos.add(l.strip())


def get_list_of_files(dir_path):
    return [f for f in listdir(dir_path) if isfile(
        join(dir_path, f)) and f.endswith('json')]


def parse_file(filename):
    filename = join(dir_path, filename)
    try:
        f = file(filename)
    except IOError:
        print ("Can not read file `{}`".format(filename))
        sys.exit(1)

    def flatten(struct, prefix=''):
        """
        Turns {'key': {'k1': 'v1', 'k2': 'v2'}} into
        {'key.k1': 'v1', 'key.k2': 'v2'}
        """
        new = {}
        for (key, val) in struct.items():
            if isinstance(val, dict):
                new.update(flatten(val, key + '.'))
            else:
                new[prefix + key] = val
        return new

    # first pass, collecting all the field names
    all_fields = set()
    for line in f:
        s = json.loads(line)
        if s['type'] in ('WatchEvent', 'FollowEvent', 'ForkEvent'):
            all_fields |= set(flatten(s).keys())

    # actually writing CSV file
    f.seek(0)
    with open(filename.replace('json', 'csv'), 'w') as csvfile:
        # fieldnames = sorted(list(all_fields))
        fieldnames = ('actor.login', 'type', 'created_at', 'repo.name')
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for line in f:
            s = flatten(json.loads(line))
            # delete all extra fields
            for k in s.keys():
            	if k not in fieldnames:
            		del s[k]
            if (s['type'] in ('WatchEvent', 'FollowEvent', 'ForkEvent') 
            	and s['actor.login'] in active_users 
            	and s['repo.name'] in popular_repos):
                writer.writerow(s)

json_names = get_list_of_files(dir_path)
pool = Pool(processes=8)
pool.map(parse_file, json_names)