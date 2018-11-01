"""
Data linkage across 2 data sets
(for small sets as in memory processing)
"""
import requests
import os
import re
import logging

import dedupe


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
    # exit(1)

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

raw_data1 = requests.get("{}/publishers/{}/entities".format(INSTANCE, SOURCE1),
                         headers={'Authorization': 'Bearer {}'.format(JWT)}).json()

raw_data2 = requests.get("{}/publishers/{}/entities".format(INSTANCE, SOURCE2),
                         headers={'Authorization': 'Bearer {}'.format(JWT)}).json()

data_set1 = read_data(raw_data1)
data_set2 = read_data(raw_data2)

del raw_data1
del raw_data2


settings_file = 'salesforce-contact-kss-4d-technology-innovator'
training_file = 'data_matching_training.json'

if os.path.exists(settings_file):
    print('reading from', settings_file)
    with open(settings_file, 'rb') as sf:
        linker = dedupe.StaticRecordLink(sf)
else:
    # we need to have same key fields name for both data sets
    fields = [
        {'field': 'Name', 'type': 'String', 'has missing': True},
        {'field': 'Email', 'type': 'String', 'has missing': True},
        {'field': 'MobilePhone', 'type': 'String', 'has missing': True}
    ]

    linker = dedupe.RecordLink(fields)

    linker.sample(data_set1, data_set2, 15000)

    logging.info("Start active labeling")

    dedupe.consoleLabel(linker)
    linker.train()

    with open(training_file, 'w') as tf:
        linker.writeTraining(tf)

    with open(settings_file, 'wb') as sf:
        linker.writeSettings(sf)


logging.info('clustering...')
linked_records = linker.match(data_set2, data_set1, 0)

logging.info('%s duplicate sets', len(linked_records))

cluster_membership = {}
cluster_id = None
for cluster_id, (cluster, score) in enumerate(linked_records):
    for record_id in cluster:
        cluster_membership[record_id] = (cluster_id, score)

print(cluster_membership)



