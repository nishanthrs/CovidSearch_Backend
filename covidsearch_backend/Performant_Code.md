### Async Programming:
Asynchronous programming perfect for running I/O heavy workloads in parallel on one thread and process  
Instead of CPU having to wait for I/O call to complete, it can resume executing other tasks or subsequent lines in program  
Drives better CPU utilization and effectively increases program's throughput  
Any I/O heavy task should be implemented in an async method:  
DB[MySQL, Elasticsearch, Redis] read and write queries  
API calls  
Pretty much ANY network call  
Reading a large file in disk (a bit outdated though)  

```await``` on async method call waits for async to finish executing; necessary if subsequent code uses that result  
However, if subsequent code is independent of that result or there are multiple **independent** I/O heavy tasks, we can use asynchronous programming to optimize this and run them in parallel  

Example:  
```
ig_profile_tasks = [
    query_num_followers(username),
    query_num_posts(username),
    load_profile_pic(username)
]
num_followers, num_posts, profile_pic = await asyncio.gather(*ig_profile_tasks)
```  

Be wary of executing too many asynchronous calls. *What's even better than async programming is simply cutting out any unnecessary I/O operations.*    
For example, if you have too many asynchronous queries to the db, there's a good chance the db will timeout b/c of the resulting excess in connections. It's MUCH more efficient to batch these asynchronous queries into one or a few queries, cutting out many round trips to the db and leading to fewer timeouts and much lower latency. The same goes for making calls to another API, especially third-party ones with rate limits.  

### Optimizing Django Framework:  
https://developer.mozilla.org/en-US/docs/Learn/Server-side/Django/Deployment  
https://www.nginx.com/blog/deploying-nginx-nginx-plus-docker/  

### Caching DB Queries:  

### Optimal DB Design:  
