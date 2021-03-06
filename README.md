# Covid19 Search API
[Kaggle competition](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge) to extract insights from research papers using novel NLP techniques  

## Preprocessing Pipeline 

[Dataset Website](https://www.semanticscholar.org/cord19)  

1. Download dataset (research papers and embeddings):  

    ```bash
    chmod u+x download_cord_19_dataset.sh
    ./download_cord_19_dataset
    ```  

2. Start the docker containers by running the docker-compose file:  

    ```bash
    sudo `which docker-compose` up -d --build
    # Confirm the containers are running successfully
    sudo docker ps -a
    ```

3. Build the search index with the dataset of research papers:  

    ```bash
    python3 covidsearch_backend/covidsearch_backend/scripts/build_research_paper_index.py
    ```

If you need to delete the search index and its contents for whatever reason, then run `python3 covidsearch_backend/covidsearch_backend/scripts/delete_index.py`.  
If you need to deduplicate the search index b/c there might be duplicate docs, then run `python3 covidsearch_backend/covidsearch_backend/scripts/deduplicate_docs.py`.  

TODO: Need to figure out how to upsert entries to Elasticsearch index so that the `build_research_paper_index.py` script can be run multiple times w/o need to delete or deduplicate index.  

## Search Methodology #1

The first and most simple way to query research papers is just regular old search powered by Elasticsearch.  
I decided to configure fuzzy search with very basic field boosting in the first iteration of the Elasticsearch query.  

```json
"multi_match": {
    "query": "Search query",
    "fields": ["title^2", "abstract", "body"],
    "fuzziness": "AUTO",
    "operator": "AND",
}
```

## Search Methodology #2  

The second way to query research papers is using a vector-space model via its tf-idf vectors.  
The tf-idf vectors are generated by first taking the term-frequency of a document and then calculating each word's inverse document frequency, weighing words that appear in less documents more. The size of the vector is equal to the size of the vocabulary (# unique tokens) of all the documents.   
Step 1: Fit and transform all documents into tf-idf vectors: `TfidfVectorizer().fit_transform(docs)`.  
Step 2: Transform search query into tf-idf vector: `TfidfVectorizer().transform([query])`.  
Step 3: Calculate the cosine similarity b/w the query vector and all the doc vectors. Sort by closest:  

```python3
results = cosine_similarity(X,query_vec)
for i in results.argsort()[-10:][::-1]:
    print(df.iloc[i,0],"--",df.iloc[i,1])
```  

Custom Python Script for Elasticsearch Ranking: https://stackoverflow.com/questions/20974964/python-custom-scripting-in-elasticsearch  

Read more about CORS here (restricting clients that can call this API):  
https://pypi.org/project/django-cors-headers/
