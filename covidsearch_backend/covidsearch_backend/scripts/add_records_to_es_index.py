#!/usr/bin/python3

from logging import log
from elasticsearch import Elasticsearch, RequestError
import json
import pandas as pd
import os
from pprint import pprint
import spacy
import time

nlp = spacy.load("en_core_web_sm")

COVID19_PAPERS_INDEX = "covid19_papers"
DATA_TYPE = "record"
UPLOAD_CHUNK_SIZE = 1000


def rec_to_actions(df, index, data_type):
    for record in df.to_dict(orient="records"):
        yield ('{ "index" : { "_index" : "%s", "_type" : "%s" }}' % (index, data_type))
        yield (json.dumps(record))


def upload_papers_to_es_idx(
    papers_df, es_idx, es_hosts, data_type=DATA_TYPE, chunk_size=UPLOAD_CHUNK_SIZE
):
    """
    Uploading Pandas DF to Elasticsearch Index: https://stackoverflow.com/questions/49726229/how-to-export-pandas-data-to-elasticsearch
    """
    try:
        es = Elasticsearch(hosts=es_hosts)
        es.indices.create(index=es_idx, ignore=400)
    except RequestError:
        print(f"Index {es_idx} already exists; continue uploading papers to {es_idx}")
    # TODO: Catch other exceptions in the future: https://elasticsearch-py.readthedocs.io/en/master/exceptions.html

    idx = 0
    while idx < papers_df.shape[0]:
        if idx + chunk_size < papers_df.shape[0]:
            max_idx = idx + chunk_size
        else:
            max_idx = papers_df.shape[0]
        print("Range: ", idx, max_idx)

        r = es.bulk(rec_to_actions(papers_df[idx:max_idx], es_idx, data_type))
        print(not r["errors"])

        idx = max_idx


def main():
    covid19_papers = pd.read_csv("../../../treatment_papers_cleaned.csv")
    upload_papers_to_es_idx(covid19_papers, COVID19_PAPERS_INDEX, ["localhost"])


if __name__ == "__main__":
    main()
