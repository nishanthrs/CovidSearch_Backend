#!/usr/bin/python3

import dask.dataframe
from datetime import datetime
from dateutil import parser
from elasticsearch import Elasticsearch, RequestError, TransportError
import json
import numpy as np
import os
import pandas as pd
from pprint import pprint
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from sklearn.feature_extraction.text import TfidfVectorizer
import time
from typing import List

NUM_DF_PARTITIONS = 30
RESEARCH_PAPER_DATA_DIR = "research_papers"
COVID19_PAPERS_INDEX = "covid19_papers"
DATA_TYPE = "research_paper"
UPLOAD_CHUNK_SIZE = 1000
DEEP_EMBEDDINGS_MAP = {
    "cord19": "<insert_filepath_here>",  # 768-length vector
    "distilbert": "<insert_filepath_here>",  # 256-length vector
    "bert": "<insert_filepath_here>",  # 768-length vector (1024 for large)
}


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


def remove_papers_with_null_cols(df: pd.DataFrame, cols: List[str]) -> None:
    df.dropna(subset=cols, how="all", inplace=True)


def fill_in_missing_data(df: pd.DataFrame) -> None:
    cols_with_missing_vals = df.columns[df.isna().any()].tolist()
    print(f"Cols with missing values: {cols_with_missing_vals}")
    df.fillna("", inplace=True)
    cols_with_missing_vals = df.columns[df.isna().any()].tolist()
    print(f"Cols with missing values after: {cols_with_missing_vals}")


def generate_embeddings(embedding_type: str, docs: pd.Series) -> pd.Series:
    if embedding_type == "tfidf":
        tfidf_vectorizer = TfidfVectorizer()
        doc_embeddings = tfidf_vectorizer.fit_transform(docs)
        print(f"Title embeddings shape: {doc_embeddings.toarray()}")
    else:
        try:
            # TODO: Correctly load and generate embeddings using CORD19 embeddings and HuggingFace transformers lib
            embeddings = DEEP_EMBEDDINGS_MAP[embedding_type]
        except KeyError:
            raise KeyError(
                f"Embedding type {embedding_type} nonexistent in embedding map: {DEEP_EMBEDDINGS_MAP}. Make sure the embedding type exists as a key in the embedding map."
            )

    return doc_embeddings


def preprocess_papers(metadata_filename) -> pd.DataFrame:
    # Change current working dir
    os.chdir("../../../cord_19_dataset")

    # Get metadata of research papers
    metadata_cols = list(pd.read_csv(metadata_filename, nrows=0).columns)
    metadata_cols_dtypes = {col: str for col in metadata_cols}
    metadata_df = pd.read_csv(metadata_filename, dtype=metadata_cols_dtypes)
    print(f"Metadata df shape: {metadata_df.shape}")
    print(f"Memory usage of metadata_df: {metadata_df.memory_usage(deep=True).sum()}")
    remove_papers_with_null_cols(metadata_df, ["title"])
    remove_papers_with_null_cols(metadata_df, ["abstract", "url"])
    fill_in_missing_data(metadata_df)
    metadata_dd = dask.dataframe.from_pandas(metadata_df, npartitions=NUM_DF_PARTITIONS)
    print(f"Memory usage of metadata_df: {metadata_df.memory_usage(deep=True).sum()}")

    # Get body of research papers and store in df
    research_papers_df = gather_papers_data(metadata_df, metadata_dd, os.getcwd())
    print(
        f"Memory usage of research_papers_df: {research_papers_df.memory_usage(deep=True).sum()}"
    )
    # TODO: MemoryError here; figure out way to prevent this
    research_papers_dd = dask.dataframe.from_pandas(
        research_papers_df, npartitions=NUM_DF_PARTITIONS
    )

    # Get embeddings of each research paper's title and abstract (embeddings of body text would lose too much info due to current ineffective pooling techniques)
    """
    embedding_type = "tfidf"
    research_papers_df[f"title_{embedding_type}"] = research_papers_dd.map_partitions(
        lambda df: generate_embeddings(embedding_type, df["title"])
    ).compute(scheduler="processes")
    research_papers_df[f"abstract_{embedding_type}"] = research_papers_dd.map_paritions(
        lambda df: generate_embeddings(embedding_type, df["abstract"])
    ).compute(scheduler="processes")
    """

    print(f"Research papers df shape: {research_papers_df.shape}")

    return research_papers_df


def rec_to_actions(df, index, data_type):
    for record in df.to_dict(orient="records"):
        record_id = record["cord_uid"]
        yield (
            '{ "index" : { "_index" : "%s", "_type" : "%s", "_id": "%s" }}'
            % (index, data_type, record_id)
        )
        yield (json.dumps(record))


def upload_papers_to_es_idx(
    papers_df, es_idx, es_hosts, data_type=DATA_TYPE, chunk_size=UPLOAD_CHUNK_SIZE
):
    """
    Uploading Pandas DF to Elasticsearch Index: https://stackoverflow.com/questions/49726229/how-to-export-pandas-data-to-elasticsearch
    """
    # TODO: Catch other exceptions in the future: https://elasticsearch-py.readthedocs.io/en/master/exceptions.html
    try:
        es = Elasticsearch(hosts=es_hosts)
        es.indices.create(index=es_idx, ignore=400)
    except RequestError:
        print(f"Index {es_idx} already exists; continue uploading papers to {es_idx}")

    try:
        idx = 0
        while idx < papers_df.shape[0]:
            if idx + chunk_size < papers_df.shape[0]:
                max_idx = idx + chunk_size
            else:
                max_idx = papers_df.shape[0]

            papers_df_chunk = papers_df.loc[idx:max_idx]
            r = es.bulk(rec_to_actions(papers_df_chunk, es_idx, data_type))
            print(f"Uploaded papers from {idx} to {max_idx}")
            idx = max_idx
            print(f"Errors: {r['errors']}")
    except TransportError as te:
        transport_error_413_url = "https://github.com/elastic/elasticsearch/issues/2902"
        transport_error_429_urls = [
            "https://stackoverflow.com/questions/61870751/circuit-breaking-exception-parent-data-too-large-data-for-http-request",
            "https://github.com/elastic/elasticsearch/issues/31197",
        ]
        if te.status_code == 413:
            print(
                f"Transport error with status code 413. Chunk size is too large, so try reducing chunk size constant or increase http.max_content_length in the yml file. More info here: {transport_error_413_url}"
            )
        elif te.status_code == 429:
            print(
                f"Transport error with status code 429. Elasticsearch's JVM heap size is too small, so try increasing ES_HEAP_SIZE env var in docker-compose.yml. More info here: {transport_error_429_urls}"
            )
        print(f"Error stacktrace: {te.error, te.info}")
        raise te


def main():
    start = time.time()
    research_papers_df = preprocess_papers("metadata.csv")
    preprocessing_end = time.time()
    print(
        f"Preprocessing time (in seconds): {preprocessing_end - start}"
    )  # Takes ~60-65 seconds
    upload_papers_to_es_idx(research_papers_df, COVID19_PAPERS_INDEX, ["localhost"])
    build_es_index_end = time.time()
    print(
        f"Upload to elasticsearch idx: {build_es_index_end - preprocessing_end}"
    )  # Takes ~240-270 seconds


if __name__ == "__main__":
    main()

