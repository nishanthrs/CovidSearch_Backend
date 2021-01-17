## Build Elasticsearch Index

There were several challenges with writing this script due to the large data size:  

### Elasticsearch Transport Exceptions
There were many instances in which inserting the data in the Elasticsearch index would run into transport exceptions:  
* `413: Request Entity Too Large`: Too much data would be sent in one bulk request, greater than what's allowed in elasticsearch's config var `http.max_content_length`.  
* `429 Circuit Breaking Exception`: The Elasticsearch heap size isn't large enough for the data caching requirements.  
* To resolve these errors, we first catch them with proper error messages and details on what to do next. Then we can either send smaller partitions of data in a request or overwrite the `http.max_content_length` to 200mb (default is 100mb) to fix the 413 exception. Check the new `http.max_content_length` var by running `curl -Ss "http://localhost:9200/_cluster/settings?include_defaults=true" | grep http` in the ES host.  

### Out-of-Memory Datasets
Pandas doesn't do a great job of storing and compressing strings and as a result, when computing and adding the research papers' bodies to the metadata, the Pandas dataframe no longer fit in memory. To fix this issue, we used Dask.  

**Dask**  
Dask is a parallel computing library (one machine or distributed) that offers abstractions to **work with numpy and pandas datasets larger than a single machine's memory through parallel computation**. Numpy, pandas, and sklearn were not designed to scale beyond a single CPU or its machine's memory. Dask solves this problem by offering tools to process distributed data efficiently via simple Pythonic integration.  

Dask parallelizes computations via **partitions**. Dask dataframes are made up on many pandas dataframes (i.e. partition), loaded lazily and in parallel. A single computation on a Dask dataframe calls the corresponding pandas method on each partition, all done in parallel across partitions. This also allows us to interact with Dask dataframes larger than memory, *so long as each partition fits into a single worker's memory*. Furthermore, Dask dataframes are *lazily loaded*, meaning that operations on a Dask dataframe builds up a computation a graph, but doesn't actually execute. Only when `compute(scheduler)` is called upon the Dask dataframe does the parallel computation execute. This is the essence of lazy loading; the computation functions themselves are stored, but they aren't actually executed and the resulting output isn't actually stored in memory *until the data is accessed*. They're commonly used in reading large files in data pipelines or generator computation expressions (`range(1000)`). This helps conserve memory and reduce computation runtime by only computing the necessary data.  

Thus, Dask's power comes from running intensive computations on large out-of-memory datasets in parallel with a small memory footprint.  

Breaking down Dask, we have two components:  

1. High-Level Collections: Dask provides high-level data structures that mimic `numpy (dask.numpy)` and `pandas (dask.dataframe)`, but can operate on datasets that don't fit into main memory. Basically, they're alternatives to numpy and pandas for large datasets.  
2. Low-Level Schedulers: Dask provides dynamic task schedulers that execute computation DAGs in parallel. These execution engines power computations on the high-level collections. They allow for fast computations on out-of-memory datasets with a minimal memory footprint. Basically, they're alternatives to multiprocessing, multithreading, and other task scheduling systems like Luigi.  

**Solving the Problem**  
Even with Dask, we still ran into many issues. That's because in this case, the computation expanded the dataset memory size. Usually computations retrieve aggregate or summarized data on the dataset, like mean or count by group. To solve this problem, we:  

1. Build the computation graph of the Dask dataframe. In this case, we loaded all the computations by calling the pandas methods on the dask dataframe. Examples of computations: filtering data, filling in missing values, and retrieving the research papers' bodies' text.  
2. Repartition the data to a size of 100MB for each partition.  
3. Save the Dask dataframe to Parquet via ``dd.to_parquet` using the `fastparquet` engine. This will execute the computations in parallel and save it in an optimized file format: Apache Parquet.  
4. Finally, we load the data from Parquet as a Dask dataframe. To access the data, we loop through the `dask.dataframe.npartitions` and call `compute()` (super efficient) on each partition. We upload this data to Elasticsearch, which should be under the max request size of `http.max_content_length=200mb`.  
