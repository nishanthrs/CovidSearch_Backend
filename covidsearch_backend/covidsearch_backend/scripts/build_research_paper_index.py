#!/usr/bin/python3

import dask.dataframe
from datetime import datetime
from dateutil import parser
from elasticsearch import Elasticsearch, RequestError
import json
import numpy as np
import os
import pandas as pd
from pprint import pprint
import time
from typing import List

NUM_DF_PARTITIONS = 30
RESEARCH_PAPER_DATA_DIR = "research_papers"
COVID19_PAPERS_INDEX = "covid19_papers"
DATA_TYPE = "research_paper"
UPLOAD_CHUNK_SIZE = 1000


def retrieve_paper_body_text(pdf_json_files) -> str:
    if pdf_json_files and type(pdf_json_files) is str:
        for json_path in pdf_json_files.split("; "):
            paper_body_text = []

            try:
                with open(json_path) as paper_json:
                    full_text_dict = json.load(paper_json)

                    for paragraph_dict in full_text_dict["body_text"]:
                        paragraph_text = paragraph_dict["text"]
                        section_name = paragraph_dict["section"]
                        if section_name.lower() != "abstract":
                            paper_body_text.append(paragraph_text)

                if paper_body_text:  # Stop searching through pdf_json_files
                    return "\n".join(paper_body_text)
            except FileNotFoundError as e:
                print(f"Failed on {json_path} with exception: {str(e)}")

    return ""


def gather_papers_data(
    metadata_df: pd.DataFrame, metadata_dd: dask.dataframe, dir: str
) -> pd.DataFrame:
    metadata_df["body"] = metadata_dd.map_partitions(
        lambda df: df["pdf_json_files"].apply(
            lambda pdf_json_files: retrieve_paper_body_text(pdf_json_files)
        )
    ).compute(scheduler="processes")
    return metadata_df


def clean_papers_data(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    return df.dropna(subset=cols)


def fill_in_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    cols_with_missing_vals = df.columns[df.isna().any()].tolist()
    print(f"Cols with missing values: {cols_with_missing_vals}")
    df = df.fillna("")
    cols_with_missing_vals = df.columns[df.isna().any()].tolist()
    print(f"Cols with missing values after: {cols_with_missing_vals}")
    return df


def preprocess_papers(metadata_filename) -> pd.DataFrame:
    os.chdir("../../../cord_19_dataset")
    # Get metadata of research papers
    metadata_cols = list(pd.read_csv(metadata_filename, nrows=0).columns)
    metadata_cols_dtypes = {col: str for col in metadata_cols}
    metadata_df = pd.read_csv(metadata_filename, dtype=metadata_cols_dtypes)
    metadata_df = clean_papers_data(metadata_df, ["title"])
    metadata_df = fill_in_missing_data(metadata_df)
    metadata_dd = dask.dataframe.from_pandas(metadata_df, npartitions=NUM_DF_PARTITIONS)
    # Get body of research papers and store in df
    research_papers_df = gather_papers_data(metadata_df, metadata_dd, os.getcwd())
    print(f"Research papers df shape: {research_papers_df.shape}")
    return research_papers_df


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

    # If not split into chunks, 413 exception is thrown: https://github.com/elastic/elasticsearch/issues/2902
    idx = 0
    while idx < papers_df.shape[0]:
        if idx + chunk_size < papers_df.shape[0]:
            max_idx = idx + chunk_size
        else:
            max_idx = papers_df.shape[0]

        papers_df_chunk = papers_df.loc[idx:max_idx]
        r = es.bulk(rec_to_actions(papers_df_chunk, es_idx, data_type))
        print(f"Uploaded papers from {idx} to {max_idx}")
        print(f"No errors: {not r['errors']}")

        idx = max_idx


def main():
    start = time.time()
    research_papers_df = preprocess_papers("metadata.csv")
    preprocessing_end = time.time()
    print(
        f"Preprocessing time (in seconds): {preprocessing_end - start}"
    )  # Takes around ~60-65 seconds)
    upload_papers_to_es_idx(research_papers_df, COVID19_PAPERS_INDEX, ["localhost"])
    build_es_index_end = time.time()
    print(f"Upload to elasticsearch idx: {build_es_index_end - preprocessing_end}")


if __name__ == "__main__":
    main()

