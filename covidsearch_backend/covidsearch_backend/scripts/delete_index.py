#!/usr/bin/python3

from elasticsearch import Elasticsearch
from add_records_to_es_index import COVID19_PAPERS_INDEX


def delete_idx(es_hosts, idx_name) -> None:
    es = Elasticsearch(hosts=es_hosts)
    es.indices.delete(index="test-index", ignore=[400, 404])


def main():
    delete_idx(["localhost"], COVID19_PAPERS_INDEX)


if __name__ == "__main__":
    main()
