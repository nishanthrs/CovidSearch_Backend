#!/usr/bin/python3
from typing import List

from elasticsearch import Elasticsearch
from build_research_paper_index import COVID19_PAPERS_INDEX


def delete_idx(es_hosts: List[str], idx_name: str) -> None:
    es = Elasticsearch(hosts=es_hosts)
    es.indices.delete(index=idx_name, ignore=[400, 404])


def main():
    es_hosts = ["localhost"]
    delete_idx(es_hosts, COVID19_PAPERS_INDEX)
    print(f"Successfully deleted index: {COVID19_PAPERS_INDEX} on hosts: {es_hosts}!")


if __name__ == "__main__":
    main()
