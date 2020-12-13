#!/usr/bin/python3

import json
import numpy as np
import os
import pandas as pd
from pprint import pprint
import spacy
import time
import dask.dataframe

NUM_DF_PARTITIONS = 30


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


def filter_paper_by_keywords(text, keywords):
    text = [word.lower().strip() for word in text.split(" ")]
    for keyword in keywords:
        keyword_parts = keyword.split(" ")
        if any(word in text for word in keyword_parts):
            return True
    return False


def main():
    # Setup models and cwds
    nlp = spacy.load("en_core_web_sm")
    os.chdir("../../../cord_19_dataset")
    print(f"CWD: {os.getcwd()}")
    # Get metadata of research papers
    metadata_df = pd.read_csv("metadata.csv")
    metadata_dd = dask.dataframe.from_pandas(metadata_df, npartitions=NUM_DF_PARTITIONS)
    # Get body of research papers and store in df
    papers_df = gather_papers_data(metadata_df, metadata_dd, os.getcwd())
    print(f"Papers df size: {papers_df.shape}")
    print(f"Papers df head: {papers_df.head()}")
    # Create a Dask dataframe of research papers
    # papers_data = dask.dataframe.from_pandas(papers_df, npartitions=NUM_DF_PARTITIONS)
    # TODO: Perform further preprocessing steps here if need be


if __name__ == "__main__":
    main()
