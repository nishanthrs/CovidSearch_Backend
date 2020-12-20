import asyncio
import json
import logging
import os

from django.http import HttpRequest, HttpResponseNotAllowed, JsonResponse
from elasticsearch import Elasticsearch, AsyncElasticsearch
from typing import Any, Dict, List, Optional, Tuple, Union

NUM_INITIAL_SEARCH_RESULTS = 100


async def _search_elasticsearch_index(hosts: List[str], index: str, query: str) -> Dict:
    """
    Asynchronous method to search papers in elasticsearch
    """
    try:
        es = AsyncElasticsearch(hosts=hosts)
        # Simple search by title
        search_tasks = []
        title_match_query = {"match": {"title": {"query": query}}}
        regular_match_query = {
            "multi_match": {"query": query, "fields": ["title", "abstract", "body"],}
        }
        fuzzy_match_query = {"fuzzy": {"title": {"value": query, "fuzziness": "AUTO"}}}
        search_coroutine = es.search(
            index="covid19_papers",
            body={
                "from": 0,
                "size": NUM_INITIAL_SEARCH_RESULTS,
                "query": regular_match_query,
            },
        )
        search_tasks.append(search_coroutine)
        # Just add more I/O heavy tasks (like db queries!) to search_tasks to get full benefit of async concurrency
        (res,) = await asyncio.gather(*search_tasks)

        relevant_papers = []
        for paper in res["hits"]["hits"]:
            # TODO: Create object/model for paper response
            paper_data = paper["_source"]
            relevant_papers.append(
                {
                    "title": paper_data["title"],
                    "authors": paper_data["authors"],
                    "abstract": paper_data["abstract"],
                    "body": paper_data["body"],
                    "url": paper_data["url"],
                }
            )
    except Exception as exc:
        # TODO: Haven't decided how to handle errors in Elasticsearch
        raise exc

    return relevant_papers


def search_covid19_papers(request: HttpRequest) -> JsonResponse:
    hosts = [
        ":".join([os.environ["ES_HOST"], os.environ["ES_PORT"]]),
        "localhost:9200",
    ]  # IP address or domain name of Elasticsearch index
    index = "covid19_papers"
    query = request.GET["query"]

    # Run async method in synchronous method by creating event loop
    loop = asyncio.new_event_loop()
    relevant_papers = loop.run_until_complete(
        _search_elasticsearch_index(hosts, index, query)
    )
    loop.close()
    return JsonResponse(data={"status": 200, "papers": relevant_papers})
