import json
import logging
from elasticsearch import Elasticsearch
from typing import Any, Dict, List, Optional, Tuple, Union

from django.http import HttpRequest, HttpResponseNotAllowed, JsonResponse


NUM_INITIAL_SEARCH_RESULTS = 100


def search_covid19_papers(request: HttpRequest) -> JsonResponse:
    try:
        query = request.GET["query"]

        es = Elasticsearch(hosts=["localhost"])
        # Simple search by title
        res = es.search(
            index="covid19_papers",
            body={
                "from": 0,
                "size": NUM_INITIAL_SEARCH_RESULTS,
                "query": {"match": {"title": {"query": query}}},
            },
        )

        relevant_papers = []
        for paper in res["hits"]["hits"]:
            # TODO: Create object/model for paper response
            paper_data = paper["_source"]
            relevant_papers.append(
                {
                    "paper_id": paper_data["paper_id"],
                    "title": paper_data["title"],
                    "abstract": paper_data["abstract"],
                }
            )
    except Exception as exc:
        # TODO: Haven't decided how to handle errors in Elasticsearch
        raise exc

    return JsonResponse({"status": 200, "papers": relevant_papers})
