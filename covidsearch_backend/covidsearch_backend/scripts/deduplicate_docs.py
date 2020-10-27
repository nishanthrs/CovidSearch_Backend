#!/usr/local/bin/python3

# https://www.elastic.co/blog/how-to-find-and-remove-duplicate-documents-in-elasticsearch

import hashlib
from elasticsearch import Elasticsearch
from add_records_to_es_index import COVID19_PAPERS_INDEX, DATA_TYPE

es = Elasticsearch(["localhost"])
dict_of_duplicate_docs = {}
# The following line defines the fields that will be
# used to determine if a document is a duplicate
keys_to_include_in_hash = ["paper_id"]
# Process documents returned by the current search/scroll
def populate_dict_of_duplicate_docs(hits):
    for item in hits:
        combined_key = ""
        for mykey in keys_to_include_in_hash:
            combined_key += str(item["_source"][mykey])
        _id = item["_id"]
        hashval = hashlib.md5(combined_key.encode("utf-8")).digest()
        # If the hashval is new, then we will create a new key
        # in the dict_of_duplicate_docs, which will be
        # assigned a value of an empty array.
        # We then immediately push the _id onto the array.
        # If hashval already exists, then
        # we will just push the new _id onto the existing array
        dict_of_duplicate_docs.setdefault(hashval, []).append(_id)


# Loop over all documents in the index, and populate the
# dict_of_duplicate_docs data structure.
def scroll_over_all_docs():
    data = es.search(
        index=COVID19_PAPERS_INDEX, scroll="1m", body={"query": {"match_all": {}}}
    )
    # Get the scroll ID
    sid = data["_scroll_id"]
    scroll_size = len(data["hits"]["hits"])
    # Before scroll, process current batch of hits
    populate_dict_of_duplicate_docs(data["hits"]["hits"])
    while scroll_size > 0:
        data = es.scroll(scroll_id=sid, scroll="2m")
        # Process current batch of hits
        populate_dict_of_duplicate_docs(data["hits"]["hits"])
        # Update the scroll ID
        sid = data["_scroll_id"]
        # Get the number of results that returned in the last scroll
        scroll_size = len(data["hits"]["hits"])


def loop_over_hashes_and_remove_duplicates():
    # Search through the hash of doc values to see if any
    # duplicate hashes have been found
    for hashval, array_of_ids in dict_of_duplicate_docs.items():
        if len(array_of_ids) > 1:
            # print("********** Duplicate docs hash=%s **********" % hashval)
            # Get the documents that have mapped to the current hashval
            matching_docs = es.mget(
                index=COVID19_PAPERS_INDEX,
                doc_type=DATA_TYPE,
                body={"ids": array_of_ids},
            )
            # print(f"# matching docs: {len(matching_docs)}")
            doc_ids = [doc["_id"] for doc in matching_docs["docs"]]
            """
            for doc in matching_docs["docs"]:
                # In this example, we just print the duplicate docs.
                # This code could be easily modified to delete duplicates
                # here instead of printing them
                # print("doc=%s\n" % doc)
                doc_id = doc["_id"]
                es.delete(index=COVID19_PAPERS_INDEX, doc_type=DATA_TYPE, id=doc_id)
            """

            # Delete in bulk; much faster operation than deleting one by one: https://stackoverflow.com/questions/30859142/how-to-delete-documents-from-elasticsearch
            bulk_delete_body = [
                '{{"delete": {{"_index": "{}", "_type": "{}", "_id": "{}"}}}}'.format(
                    COVID19_PAPERS_INDEX, DATA_TYPE, doc_id
                )
                for doc_id in doc_ids
            ]
            es.bulk("\n".join(bulk_delete_body))


def main():
    pre_total_num_hits = es.count(
        index=COVID19_PAPERS_INDEX, body={"query": {"match_all": {}}}
    )
    scroll_over_all_docs()
    loop_over_hashes_and_remove_duplicates()
    post_total_num_hits = es.count(
        index=COVID19_PAPERS_INDEX, body={"query": {"match_all": {}}}
    )
    print(
        f"Number of entries before and after deduplication: {pre_total_num_hits}, {post_total_num_hits}"
    )


main()
