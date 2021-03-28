import asyncio
import json
import logging
import os

from django.http import HttpRequest, HttpResponseNotAllowed, JsonResponse
from elasticsearch import Elasticsearch, AsyncElasticsearch
from sklearn.feature_extraction.text import TfidfVectorizer
from typing import Any, Dict, List, Optional, Tuple, Union

NUM_INITIAL_SEARCH_RESULTS = 20


async def _search_elasticsearch_index(hosts: List[str], index: str, query: str) -> Dict:
    """
    Asynchronous method to search papers in elasticsearch
    """
    try:
        es = AsyncElasticsearch(hosts=hosts)
        # Simple search by title
        search_tasks = []
        # title_match_query = {"match": {"title": {"query": query}}}
        fuzzy_multimatch_query = {
            "multi_match": {
                "query": query,
                "fields": ["title^2", "abstract", "body"],
                "fuzziness": "AUTO",
                "operator": "AND",
            }
        }
        # TODO: Modify below query to use with preprocessed embeddings from build_research_paper_index script
        # Use source: https://sci2lab.github.io/ml_tutorial/tfidf/#Extra-Resources
        """
        tfidf_vectorizer = pickle.load(open("models/tfidf_title_vectorizer.pickle", "rb"))
        query_vector = tfidf_vectorizer.transform(query)
        script_query = {
            "script_score": {
                "query": {
                    "match_all": {}
                },  # TODO: Maybe replace with fuzzy_multimatch_query since we're using embeddings mainly for better ranking, not better discoverability?
                "script": {
                    "source": "cosineSimilarity(params.query_vector, doc['text_vector']) + 1.0",
                    "params": {"query_vector": query_vector},
                },
            }
        }
        """
        # TODO: Modify below query to use with preprocessed BERT (or other deep learning models) embeddings from build_research_paper_index script
        # Source: https://github.com/Hironsan/bertsearch/blob/master/web/app.py
        """
        bc = BertClient(ip='bertserving', output_fmt='list')
        query_vector = bc.encode([query])[0]
        script_query = {
            "script_score": {
                "query": {"match_all": {}},
                "script": {
                    "source": "cosineSimilarity(params.query_vector, doc['text_vector']) + 1.0",
                    "params": {"query_vector": query_vector}
                }
            }
        }
        """

        # TODO: Figure out way to prioritize ranking for papers with non-empty abstract and urls
        search_coroutine = es.search(
            index="covid19_papers",
            body={"from": 0, "size": NUM_INITIAL_SEARCH_RESULTS, "query": fuzzy_multimatch_query,},
        )
        search_tasks.append(search_coroutine)
        # Just add more I/O heavy tasks (like db queries!) to search_tasks to get full benefit of async concurrency
        (res,) = await asyncio.gather(*search_tasks)

        relevant_papers = []
        print(f"First result: {res['hits']['hits'][0]}")
        for paper in res["hits"]["hits"]:
            paper_relevance_score = paper["_score"]
            paper_data = paper["_source"]
            relevant_papers.append(
                {
                    "title": paper_data["title"],
                    "authors": paper_data["authors"],
                    "abstract": paper_data["abstract"],
                    "body": paper_data["body"],
                    "url": paper_data["url"],
                    "publish_time": paper_data["publish_time"],
                    "score": paper_relevance_score,
                }
            )
    except Exception as exc:
        # TODO: Haven't decided how to handle errors in Elasticsearch
        # Raise IndexNotFound exception on 404 status code Not Found
        raise exc

    return relevant_papers


def search_covid19_papers(request: HttpRequest) -> JsonResponse:
    hosts = [
        ":".join([os.environ["ES_HOST"], os.environ["ES_PORT"]]),
        "localhost:9200",
    ]  # IP address or domain name of Elasticsearch index
    index = "covid19_papers"
    query = request.GET["query"].lower()

    # Run async method in synchronous method by creating event loop
    loop = asyncio.new_event_loop()
    relevant_papers = loop.run_until_complete(_search_elasticsearch_index(hosts, index, query))
    loop.close()
    return JsonResponse(data={"status": 200, "papers": relevant_papers})
