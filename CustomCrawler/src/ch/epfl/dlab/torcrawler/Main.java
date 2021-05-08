/**
 * 
 */
package ch.epfl.dlab.torcrawler;

import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Antoine Masanet
 *
 * Reads seed urls from a file and launches multiple crawling threads
 * Can be used to start a new crawl from the seed files or recover from a previous 
 * crawl where urls in the seed file will be added to the recovered queue
 *
 */
public class Main {
	
	private static int threadCount;
	public static final int MAX_THREAD_COUNT = 100;

	/**
	 * Launches a crawl
	 * @param args: 2 options
	 * 1st: "create" + "true/false" (use of cookies) + threadCount
	 * 2nd: "restore" (threadCount will be infered from the restore file) + "true/false" (use of cookies)
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		
		
		if (args.length<2 || args.length>3) {
			throw new IllegalArgumentException("At least 2 arguments and most 3 arguments are required, see the list of required arguments in the Javadoc");
		}
		
		RoundRobinBlockingQueue urlsToFetch = null;
		Set<String> discoveredURLs=ConcurrentHashMap.newKeySet();
		
		
		if(args[0].equals("restore")) {
			System.out.println("Trying to restore roundRobinQueue");
			if(args.length!=2) {
				throw new IllegalArgumentException("Calling the crawler in restore mode should take 2 arguments");
			}
			
			urlsToFetch = RoundRobinBlockingQueue.restore();
			Set<String> recoveredURLs = null;
			try {
				recoveredURLs = FileUtility.fetchDiscoveredURLs();
				discoveredURLs.addAll(recoveredURLs);
			} catch (FileNotFoundException e) {
				throw new IllegalStateException("The discovered urls file could not be found");
			}
			
			try {
				
				CrawlerStatistics.restore(urlsToFetch);
			} catch (FileNotFoundException e) {
				throw new IllegalStateException("The Crawler statistics file could not be found");
			}
			
			threadCount = CrawlerStatistics.getInstance().getThreadCount();//Assumes the provided restore file is correct
		}
		
		else if(args[0].equals("create")) {
			if(args.length!=3) {
				throw new IllegalArgumentException("Calling the crawler in create mode should take 3 arguments");
			}
			try {
				threadCount = Integer.parseInt(args[2]);
				if (threadCount<=0 || threadCount>MAX_THREAD_COUNT) {
					throw new IllegalArgumentException("The second argument should be an integer between 1 and"+MAX_THREAD_COUNT+"(included)");
				}
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("The second argument should be an integer");
			}
			
			urlsToFetch = RoundRobinBlockingQueue.create();
			CrawlerStatistics.initialize(urlsToFetch, threadCount);
		}
		
		Map<String,Map<String,String>> cookies=null;//Map<domain,Map<cookieName,cookie>>
		
		if(args[1].equals("true")) { 
			try {
				cookies = new ConcurrentHashMap<>(FileUtility.fetchCookies());
			} catch (FileNotFoundException e) {
				throw new IllegalStateException("The cookie file could not be found");
			}
		}
		else if(args[1].equals("false")) {
			cookies = new ConcurrentHashMap<>();
		}
		else {
			throw new IllegalArgumentException("The 2nd argument for the use of cookies should be true or false");
		}
		
		try {
			
			PersistenceThread persistenceThread = new PersistenceThread(discoveredURLs);
			persistenceThread.start();//Saves the status of the crawler, the recovery file for this status and the discovered urls
			
			Set<String> seedURLs = FileUtility.fetchSeedURLs()
									.stream()
									.filter(url -> URLFilter.filterURL(url))
									.collect(Collectors.toSet());
	
			urlsToFetch.addAll(seedURLs);
			discoveredURLs.addAll(seedURLs);
			
			List<FetcherThread> fetcherThreads = new LinkedList<>();
			
			for(int i=0; i<threadCount; ++i) {
				FetcherThread fetcherThread = new FetcherThread(i,urlsToFetch,discoveredURLs,cookies);
	 			fetcherThreads.add(fetcherThread);
	 			fetcherThread.start();
			}
			
		} catch (Exception e) {
			  System.err.println("An unexpected error has occurred:");
			  e.printStackTrace();
			  System.exit(1);
		}
	}
}
