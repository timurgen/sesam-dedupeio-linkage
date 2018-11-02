"""
Data linkage script across 2 data sets
(for small sets as in memory processing, or large if you have a lot of RAM)
Usage: This script takes data from 2 published datasets and sends result to target dataset
or outputs it if target is not  declared
environment: {
    JWT:            <- jwt for respective Sesam node
    KEYS            <- which properties of data will be used for matching (must be same in both datasets)
    INSTANCE        <- URL for Sesam node
    SOURCE1         <- name of first, master data set
    SOURCE2         <- name of second, slave data set
    TARGET          <- name of target data set
    SETTINGS_FILE   <- trained model for Dedupe.io engine, if None then active training will be performed.
                        May be path to file on local system or URL
}
"""

import requests
import os
import re
import logging
import json
import numpy

import dedupe


class NumpyEncoder(json.JSONEncoder):
    """
    Custom encoder of numpy datatypes
    """

    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(NumpyEncoder, self).default(obj)


def pre_process(s):
    """
    clean strings before processing
    :param s:
    :return:
    """
    if type(s) != str:
        return s

    s = re.sub('\n', ' ', s)
    s = re.sub('-', '', s)
    s = re.sub('/', ' ', s)
    s = re.sub("'", '', s)
    s = re.sub(",", '', s)
    s = re.sub(":", ' ', s)
    s = re.sub('  +', ' ', s)
    s = s.strip().strip('"').strip("'").lower().strip()
    if not s:
        s = None
    return s


def read_data(raw_data: list) -> list:
    """
    reads raw data list -> perform some data cleaning and return
    :param raw_data:
    :return:
    """
    cleaned_data = {}
    for row in raw_data:
        # don't need deleted properties
        if row['_deleted']:
            continue
        cleaned_row = dict([(k, pre_process(v)) for (k, v) in row.items()])
        cleaned_data[row['_id']] = cleaned_row
    return cleaned_data


# Logging
logging.getLogger().setLevel(logging.DEBUG)

# token for node access
JWT = os.environ.get('JWT')

if JWT is None:
    logging.error('jwt token missing. Add environment variable \'JWT\' with token string.')
    exit(1)

# List with keys that need to be analysed for duplicate values
# ex. ['Email', 'FirstName', 'LastName', 'Phone']
# or pass as comma separated String
KEYS = os.environ.get('KEYS', [])

if isinstance(KEYS, str):
    KEYS = [x.strip() for x in KEYS.split(',')]

if not KEYS:
    logging.error("No keys for analysis were found, checking will not be possible.")
    exit(1)

# full API URL like 'https://datahub-xxxxxxx.sesam.cloud/api'
# we support only same source<->target instance
INSTANCE = os.environ.get('INSTANCE')

# first dataset for record linkage
SOURCE1 = os.environ.get('SOURCE1')
if not SOURCE1:
    logging.error("Dataset 1 for record linkage is not provided")
    exit(1)

# second set for record linkage
SOURCE2 = os.environ.get('SOURCE2')
if not SOURCE2:
    logging.error("Dataset 2 for record linkage is not provided")
    exit(1)

# target dataset
TARGET = os.environ.get('TARGET')

ADD_ORIGINALS = os.environ.get('ADD_ORIGINALS', True)

SETTINGS_FILE = os.environ.get('SETTINGS', "__settings_file.bin")
if SETTINGS_FILE.startswith('http'):
    logging.info("Found URL, retrieving trained model")
    r = requests.get(SETTINGS_FILE, stream=True)
    temp_file_name = '__settings_file'
    with open(temp_file_name, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    SETTINGS_FILE = temp_file_name

raw_data1 = requests.get("{}/publishers/{}/entities".format(INSTANCE, SOURCE1),
                         headers={'Authorization': 'Bearer {}'.format(JWT)}).json()

raw_data2 = requests.get("{}/publishers/{}/entities".format(INSTANCE, SOURCE2),
                         headers={'Authorization': 'Bearer {}'.format(JWT)}).json()

data_set1 = read_data(raw_data1)
data_set2 = read_data(raw_data2)

if os.path.exists(SETTINGS_FILE):
    print('reading from', SETTINGS_FILE)
    with open(SETTINGS_FILE, 'rb') as sf:
        linker = dedupe.StaticRecordLink(sf)
else:
    # we need to have same key fields name for both data sets
    fields = []
    for key in KEYS:
        fields.append({'field': key, 'type': 'String', 'has missing': True})

    linker = dedupe.RecordLink(fields)

    linker.sample(data_set1, data_set2, 15000)

    logging.info("Start active labeling")

    dedupe.consoleLabel(linker)
    linker.train()

    with open(SETTINGS_FILE, 'wb') as sf:
        linker.writeSettings(sf)

logging.info('clustering...')
linked_records = linker.match(data_set1, data_set2, 0.5)

logging.info('%s duplicate sets', len(linked_records))

cluster_membership = {}
cluster_id = None
for cluster_id, (cluster, score) in enumerate(linked_records):
    for record_id in cluster:
        cluster_membership[record_id] = {
            "cluster_id": cluster_id,
            "confidence_score": score
        }

result_dataset = []

for row in raw_data1:
    row_id = row['_id']
    if row_id in cluster_membership:
        result_dict = {
            '_id': row_id,
            'original_id': row['_id'],
            'is_master': True,
            'cluster_id': cluster_membership[row_id]["cluster_id"],
            'confidence_score': cluster_membership[row_id]['confidence_score']
        }
        if ADD_ORIGINALS:
            result_dict['originals'] = {key: row[key] for key in KEYS}
        result_dataset.append(result_dict)
for row in raw_data2:
    row_id = row['_id']
    if row_id in cluster_membership:
        result_dict = {
            '_id': row_id,
            'original_id': row['_id'],
            'cluster_id': cluster_membership[row_id]["cluster_id"],
            'confidence_score': cluster_membership[row_id]['confidence_score']
        }
        if ADD_ORIGINALS:
            result_dict['originals'] = {key: row[key] for key in KEYS}
        result_dataset.append(result_dict)

if TARGET is not None:
    target_url = INSTANCE + "/receivers/{}/entities".format(TARGET)
    requests.post(target_url,
                  json.dumps(sorted(result_dataset, key=lambda k: k['cluster_id']), cls=NumpyEncoder),
                  headers={'Authorization': 'Bearer {}'.format(JWT), 'Content-Type': 'application/json'})
else:
    print(json.dumps(sorted(result_dataset, key=lambda item: item['cluster_id']), cls=NumpyEncoder))
