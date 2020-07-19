from elasticsearch import Elasticsearch, RequestError
import json

COVID19_PAPERS_INDEX = "covid19_papers"
DATA_TYPE = "record"
UPLOAD_CHUNK_SIZE = 1000

# TODO: Schedule this and a coronavirus RSS feed as a chron job


def rec_to_actions(df, index, data_type):
    for record in df.to_dict(orient="records"):
        yield ('{ "index" : { "_index" : "%s", "_type" : "%s" }}' % (index, data_type))
        yield (json.dumps(record))


def upload_papers_to_es_idx(
    papers_df, es_idx, es_hosts, data_type=DATA_TYPE, chunk_size=UPLOAD_CHUNK_SIZE
):
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
