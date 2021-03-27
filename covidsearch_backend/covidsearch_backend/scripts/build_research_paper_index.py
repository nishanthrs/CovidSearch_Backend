#!/usr/bin/python3

import dask.dataframe as dd
from dask.distributed import Client
from datetime import datetime
from elasticsearch import Elasticsearch, RequestError, TransportError
import json
import multiprocessing
import numpy as np
import os
import pandas as pd
import pickle
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from sklearn.feature_extraction.text import TfidfVectorizer
import time
from typing import List

NUM_CPU_CORES = multiprocessing.cpu_count()
NUM_DF_PARTITIONS = 30
RESEARCH_PAPER_DATA_DIR = "research_papers"
COVID19_PAPERS_INDEX = "covid19_papers"
UPLOAD_CHUNK_SIZE = 1000
DEEP_EMBEDDINGS_MAP = {
    "cord19": "<insert_filepath_here>",  # 768-length vector
    "distilbert": "<insert_filepath_here>",  # 256-length vector
    "bert": "<insert_filepath_here>",  # 768-length vector (1024 for large)
}


def retrieve_paper_body_text(pdf_json_files: str) -> str:
    if pdf_json_files and type(pdf_json_files) is str:
        for json_path in pdf_json_files.split("; "):
            paper_body_text = []
            abs_json_path = os.path.join(os.getcwd(), json_path)

            try:
                with open(abs_json_path) as paper_json:
                    full_text_dict = json.load(paper_json)

                    for paragraph_dict in full_text_dict["body_text"]:
                        paragraph_text = paragraph_dict["text"]
                        section_name = paragraph_dict["section"]
                        if section_name.lower() != "abstract":
                            paper_body_text.append(paragraph_text)

                if paper_body_text:  # Stop searching through pdf_json_files
                    return "\n".join(paper_body_text)
            except FileNotFoundError as e:
                print(f"Failed on {abs_json_path} with exception: {str(e)}")

    return ""


def retrieve_paper_body_text_for_series(pdf_json_files_series: pd.Series) -> pd.Series:
    return pdf_json_files_series.apply(lambda pdf_json_files: retrieve_paper_body_text(pdf_json_files))


def gather_papers_data(metadata_dd: dd) -> dd:
    return metadata_dd.map_partitions(
        lambda df: df.assign(body=retrieve_paper_body_text_for_series(df.pdf_json_files))
    )


def remove_papers_with_null_cols(dask_df: dd, cols: List[str]) -> None:
    return dask_df.dropna(subset=cols, how="all")


def fill_in_missing_data(dask_df: dd) -> None:
    return dask_df.fillna("")


def generate_embeddings(embedding_type: str, embedding_model_filename: str, docs: pd.Series) -> pd.Series:
    if embedding_type == "tfidf":
        tfidf_vectorizer = TfidfVectorizer()
        doc_embeddings = tfidf_vectorizer.fit_transform(docs)
        with open(embedding_model_filename, "wb") as ef:
            pickle.dump(tfidf_vectorizer, ef)
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
    # Get metadata of research papers
    metadata_cols = [
        "cord_uid",
        "title",
        "authors",
        "abstract",
        "publish_time",
        "url",
        "journal",
        "pdf_json_files",
    ]
    metadata_cols_dtypes = {col: str for col in metadata_cols}
    metadata_dd = dd.read_csv(metadata_filename, dtype=metadata_cols_dtypes, usecols=metadata_cols)
    print(f"Memory usage of metadata_df before clean: {metadata_dd.memory_usage(deep=True).sum()}")
    # Perform operations in place to reduce memory usage
    metadata_dd = remove_papers_with_null_cols(metadata_dd, ["title"])
    metadata_dd = remove_papers_with_null_cols(metadata_dd, ["abstract", "url"])
    metadata_dd = fill_in_missing_data(metadata_dd)
    print(f"Memory usage of metadata_df after clean: {metadata_dd.memory_usage(deep=True).sum()}")
    print(f"# partitions in metadata dd: {metadata_dd.npartitions}")

    # Get body of research papers and store in df
    # TODO: Rename gather_papers_data and put below embedding computation logic in it
    metadata_with_body_dd = gather_papers_data(metadata_dd)
    metadata_with_body_dd = metadata_with_body_dd.repartition(partition_size="100MB")
    print(f"# partitions in research papers' metadata dd: {metadata_with_body_dd.npartitions}")
    dd.to_parquet(
        metadata_with_body_dd,
        "research_paper_bodies/",
        engine="fastparquet",
        compute_kwargs={"scheduler": "synchronous"},  # synchronous ~2m40s
    )

    # Get embeddings of each research paper's title and abstract (embeddings of body text would lose too much info due to current ineffective pooling techniques)
    """
    embedding_type = "tfidf"
    research_papers_df[f"title_{embedding_type}"] = research_papers_dd.map_partitions(
        lambda df: generate_embeddings(embedding_type, "../covidsearch_backend/covidsearch_backend/models/tfidf_title_vectorizer.pk", df["title"])
    ).compute(scheduler="processes")
    research_papers_df[f"abstract_{embedding_type}"] = research_papers_dd.map_paritions(
        lambda df: generate_embeddings(embedding_type, "../covidsearch_backend/covidsearch_backend/models/tfidf_abstract_vectorizer.pk", df["abstract"])
    ).compute(scheduler="processes")
    """


def rec_to_actions(df, index):
    for record in df.to_dict(orient="records"):
        record_id = record["cord_uid"]
        yield ('{ "index" : { "_index" : "%s", "_id": "%s" }}' % (index, record_id))
        yield (json.dumps(record))


def upload_parquet_dir_to_es_idx(parquet_dir, es_idx, es_hosts, chunk_size=UPLOAD_CHUNK_SIZE):
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
        papers_dd = dd.read_parquet(parquet_dir, engine="fastparquet")
        for partition_num in range(papers_dd.npartitions):
            start = time.time()
            papers_dd_partition = papers_dd.get_partition(partition_num)
            papers_df_partition = papers_dd_partition.compute()
            compute_end = time.time()
            print(f"Partition #{partition_num} compute time: {compute_end - start}")
            print(f"Papers partition memory size: {papers_df_partition.memory_usage(deep=True).sum()}")
            print(f"Number of papers in partition: {papers_df_partition.shape[0]}")
            r = es.bulk(rec_to_actions(papers_df_partition, es_idx))
            upload_end = time.time()
            print(f"Partition #{partition_num} upload time: {upload_end - compute_end}")
            print(f"Errors in uploading partition #{partition_num}: {r['errors']}\n\n")

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
        else:
            # TODO: Could be ConnectionTimeout in connecting to index; retry if that's the case
            print(f"Error stacktrace: {te.error, te.info}")
        raise te


def main():
    start = time.time()
    """
    NOTE: Below lines lead to cwd and file issues. 
    Investigate issue using docs here: https://docs.dask.org/en/latest/setup/single-distributed.html
    """
    # Change current working dir
    os.chdir("../../../cord_19_dataset")
    print(f"CWD: {os.getcwd()}")
    # client = Client(n_workers=NUM_CPU_CORES)  # Set to number of cores of machine
    # dask_scheduler_workers = list(client.scheduler_info()["workers"].values())
    # print(f"Workers of Dask scheduler: {dask_scheduler_workers}\n\n")
    preprocess_papers("metadata.csv")
    preprocessing_end = time.time()
    print(f"Preprocessing time (in seconds): {preprocessing_end - start}\n\n")  # Takes ~60-65 seconds
    # upload_papers_to_es_idx(research_papers_dd, COVID19_PAPERS_INDEX, ["localhost"])
    upload_parquet_dir_to_es_idx("research_paper_bodies/*", COVID19_PAPERS_INDEX, ["localhost"])
    build_es_index_end = time.time()
    print(f"Upload to elasticsearch idx: {build_es_index_end - preprocessing_end}")  # Takes ~240-270 seconds


if __name__ == "__main__":
    main()
