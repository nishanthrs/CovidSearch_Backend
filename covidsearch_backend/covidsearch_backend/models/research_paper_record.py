import json


class ResearchPaperRecord:
    def __init__(self, paper_id, paper_title, abstract, body):
        self.paper_id = paper_id
        self.paper_title = paper_title
        self.abstract = abstract
        self.body = body
