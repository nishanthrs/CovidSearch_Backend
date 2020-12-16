#!/usr/bin/python3

import json
from importlib_metadata import metadata
import numpy as np
import os
import pandas as pd
from pprint import pprint
import spacy
import time
import dask.dataframe

NUM_DF_PARTITIONS = 30
RESEARCH_PAPER_DATA_DIR = "research_papers"


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
    """
    metadata_df["body"] = metadata_df["pdf_json_files"].apply(
        lambda pdf_json_files: retrieve_paper_body_text(pdf_json_files)
    )
    """
    metadata_df["body"] = metadata_dd.map_partitions(
        lambda df: df["pdf_json_files"].apply(
            lambda pdf_json_files: retrieve_paper_body_text(pdf_json_files)
        )
    ).compute(scheduler="processes")
    return metadata_df


def fill_in_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        col_type = df[col].dtypes
        if col_type in [np.dtype("float64"), np.dtype("float32")]:
            df[col] = df[col].fillna(-1)
        else:
            df[col] = df[col].fillna("")

    return df


def standardize_col_types(df: pd.DataFrame) -> pd.DataFrame:
    """TODO: Many cols are of mixed dtype object right now, 
    which will certainly lead to issues when interacting with
    this data in the future. Make sure all data in a given col
    is of the same type!"""
    for col in df.columns:
        col_type = df[col].dtypes
        print(f"Col and its type: {col}, {col_type}")
        if col_type in [pd.np.dtype("float64"), pd.np.dtype("float32")]:
            print(f"Found float col: {col_type}")
            df[col] = df[col].fillna(-1)
        else:
            df[col] = df[col].fillna("")


def filter_paper_by_keywords(text, keywords):
    text = [word.lower().strip() for word in text.split(" ")]
    for keyword in keywords:
        keyword_parts = keyword.split(" ")
        if any(word in text for word in keyword_parts):
            return True
    return False


def main():
    # Setup models and cwds
    # nlp = spacy.load("en_core_web_sm")
    os.chdir("../../../cord_19_dataset")
    # Get metadata of research papers
    start = time.time()
    metadata_df = fill_in_missing_data(pd.read_csv("metadata.csv"))
    # metadata_df = standardize_col_types(metadata_df)
    metadata_dd = dask.dataframe.from_pandas(metadata_df, npartitions=NUM_DF_PARTITIONS)
    # Get body of research papers and store in df
    papers_df = gather_papers_data(metadata_df, metadata_dd, os.getcwd())
    end = time.time()
    print(
        f"Preprocessing time (in seconds): {end - start}"
    )  # Takes around ~59 seconds)

    if not os.path.exists(RESEARCH_PAPER_DATA_DIR):
        os.makedirs(RESEARCH_PAPER_DATA_DIR)
    papers_df.to_csv(
        os.path.join(RESEARCH_PAPER_DATA_DIR, "research_papers.csv"),
        encoding="utf-8",
        index=False,
    )
    # Create a Dask dataframe of research papers
    # papers_data = dask.dataframe.from_pandas(papers_df, npartitions=NUM_DF_PARTITIONS)
    # NOTE: Perform further preprocessing steps here if need be


if __name__ == "__main__":
    main()
