#!/usr/bin/python3

import csv
import dask.dataframe as dd
from elasticsearch import Elasticsearch, RequestError
import json
import pandas as pd
import os
from pprint import pprint
import spacy
import time
from typing import Any, List

nlp = spacy.load("en_core_web_sm")

COVID19_PAPERS_INDEX = "covid19_papers"
DATA_TYPE = "research_paper"
UPLOAD_CHUNK_SIZE = 1000
RESEARCH_PAPER_DATA_DIR = "research_papers"


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
    print("Shape: ", papers_df.shape[0].compute())
    while idx < papers_df.shape[0]:
        if idx + chunk_size < papers_df.shape[0]:
            max_idx = idx + chunk_size
        else:
            max_idx = papers_df.shape[0]

        papers_df_chunk = papers_df.loc[idx:max_idx].compute()
        r = es.bulk(rec_to_actions(papers_df_chunk, es_idx, data_type))
        print(f"Uploaded papers from {idx} to {max_idx}")
        print(not r["errors"])

        idx = max_idx


def main():
    os.chdir("../../../cord_19_dataset")
    start_time = time.time()
    """
    covid19_papers_dfs = pd.read_csv(
        os.path.join(RESEARCH_PAPER_DATA_DIR, "research_papers.csv"),
        encoding="utf-8",
        chunksize=50000,
    )
    """
    covid19_papers_dd = dd.read_csv(
        os.path.join(RESEARCH_PAPER_DATA_DIR, "research_papers.csv"),
        encoding="utf-8",
        error_bad_lines=False,
        quoting=csv.QUOTE_NONE,
        dtype={"s2_id": "object"},
    )
    end_time = time.time()
    print(f"Data load time: {end_time - start_time, type(covid19_papers_dd)}")
    upload_papers_to_es_idx(covid19_papers_dd, COVID19_PAPERS_INDEX, ["localhost"])
    end_time_2 = time.time()
    print(f"Upload to idx time: {end_time_2 - end_time}")


if __name__ == "__main__":
    main()
