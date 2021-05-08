## CustomTorCrawler
This project presents a multi-threaded web crawler on the [TOR network](https://www.torproject.org) that was used to discover 500 000 onion domains and 6 million pages.

This crawler uses a persistent queue that enables it to be stopped, and restarted from its previous state at any time.
The starting seeds are to be stored in the src/ressources/seeds.json file.
Previously generated cookies can be stored in the src/ressources/cookies.json file and used to access pages requiring a login.
In the "restore" state, the seeds file can be used to add new seeds to the existing queue.

# Launch 
To run the crawler:

- Place the desired start urls in the src/ressources/seeds.json file.
- Generate the uberjar:\
``` mvn clean compile assembly:single ```
- Launch Docker and run the Tor Proxy container:\
``` docker run -d -p 8118:8118 -p 2090:2090 -e tors=50 -e privoxy=1 --rm --name tor_proxy zeta0/alpine-tor ```
- Run the jar with the correct configurations runtype:"restore/create", threadCount :\
``` java -jar target/CustomCrawler-0.0.1-SNAPSHOT.jar create 10``` 

- To recover from a previous crawl, run the jar with "restore" as first parameter.

- The extractGraphFromData.py can be used to extract the url from the crawl data, build and save a graph from the crawl's data as well as extracting some key features of the topology of the network.

# Architecture
![alt text](images/CrawlerTopology.png)


# Results


# Technologies 
- Open JDK 11
- Docker 
- Gson
- JSoup
- [square/tape](https://github.com/square/tape)
