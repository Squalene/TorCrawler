package ch.epfl.dlab.torcrawler;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/**
 * @author Antoine Masanet
 *
 *         This class represents a Thread that fetches pages on the web by
 *         pulling the url from a queue shared amongst all threads and storing
 *         the content of this page in a compressed file. Each thread writes to
 *         its own file.
 *
 */
public class FetcherThread extends Thread {

	public final static String DATA_FOLDER = "data/pages";
	public final static String FETCH_ERROR_FOLDER = "data/urlFetchError";

	private BlockingQueue<String> urlsToFetch;// Concurrent Blocking Queue shared amongst all threads
	private Set<String> processedURLs;// Concurrent Set shared amongst all threads containing all URL that are in or
										// have been pushed to the queue
	private Map<String,Map<String,String>> cookies;//Map of domain name to cookies
	
	CompressedFileWriter pageWriter;
	CompressedFileWriter urlFetchErrorWriter;

	public final int id;

	public FetcherThread(int id, BlockingQueue<String> urlToFetch, Set<String> processedURLs, Map<String,Map<String,String>> cookies) {

		assert (urlToFetch != null);
		assert (processedURLs != null);
		this.id = id;
		this.urlsToFetch = urlToFetch;
		this.processedURLs = processedURLs;
		this.cookies=cookies;
		try {
			pageWriter = new CompressedFileWriter(DATA_FOLDER);
		} catch (IOException e) {
			System.err.println("Could not create filewriter in thread:" + id);
			e.printStackTrace();
		}

		try {
			urlFetchErrorWriter = new CompressedFileWriter(FETCH_ERROR_FOLDER);
		} catch (IOException e) {
			System.err.println("Could not create filewriter in thread:" + id);
			e.printStackTrace();
		}
	}

	/**
	 * Constantly: takes a url from a blocking queue, fetches the corresponding
	 * page and stores it to a file, as well as update the crawler statistics accordingly.
	 *
	 */
	@Override
	public void run() {
		try {
			while (true) {
				String urlToFetch = null;
				PageContent page = null;
				try {
					urlToFetch = urlsToFetch.take();
					page = Fetcher.fetchPage(urlToFetch,cookies);
				} catch (InterruptedException e1) {
					System.out.println("Interrupt when taking url from queue");
					e1.printStackTrace();
				}
	
				if (page == null) {// Could not fetch the page
					//TODO:
					System.out.println("Thread " + id + " could not fetch page:" + urlToFetch);
					try {
						urlFetchErrorWriter.save(urlToFetch);
						CrawlerStatistics.getInstance().incrementPagesFetchErrorByThread(id);
					} catch (IOException e) {
						System.err.println("Fetcher Thread :" + id + "could not store fetchError url in file");
						e.printStackTrace();
					}
				}
				else {
					CrawlerStatistics.getInstance().incrementPagesCorrectlyFetchedByThread(id);
					Set<String> urls = page.linkURLs.stream().filter(url -> URLFilter.filterURL(url))
							.collect(Collectors.toSet());
					//TODO:
					System.out.println("Thread " + id + " fetched " + urls.size() + " urls");
					System.out.println("Queue size is " + urlsToFetch.size() + " urls");
	
					enqueueURLs(urls);
	
					try {
						pageWriter.save(page);
					} catch (IOException e) {
						System.err.println("Fetcher Thread :" + id + "could not save page in file");
						e.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
			 System.err.println("An unexpected error has occurred:");
			 e.printStackTrace();
			 return;
		}
	}

	/**
	 * Add each new url to the fetch queue
	 * 
	 * @param urls: a set of url to add to the queue
	 */
	private void enqueueURLs(Set<String> urls) {
		
		urls.forEach(url -> {
			try {
				if (!processedURLs.contains(url)) {
					urlsToFetch.put(url);
					processedURLs.add(url);
					CrawlerStatistics.getInstance().incrementValidUrlsDiscovered();
				}
			} catch (InterruptedException e) {
				System.err.println("Error while putting url in the urlToFetch queue");
				e.printStackTrace();
			}
		});
	}
}
