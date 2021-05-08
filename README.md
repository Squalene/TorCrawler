## CustomTorCrawler

This crawler uses persistent queue that enable it to be stopped and restarted from its previous state at any time.
The starting seeds are to be stored in the src/ressources/seeds.json file.
Cookies can be stored in the src/ressources/cookies.json file.
In the "restore" state, the seeds file can be used to add new seeds to the existing queue.

# Launch 
To run the crawler:

- Place desired start urls in the src/ressources/seeds.json.
- Generate the uberjar:\
``` mvn clean compile assembly:single ```
- Launch Docker and run the Tor Proxy container:\
``` docker run -d -p 8118:8118 -p 2090:2090 -e tors=50 -e privoxy=1 --rm --name tor_proxy zeta0/alpine-tor ```
- Run the jar with the corrrect configurations:\
``` java -jar target/CustomCrawler-0.0.1-SNAPSHOT.jar create 10``` 

- To recover from a previous crawl, run the jar with "restore" as first parameter.

- The extractGraphFromData.py can be used to extract only the url from the crawl data, build and save a graph from the crawl's data.

# Technologies 
- Open JDK 11
- Gson
- JSoup
- square/taper
