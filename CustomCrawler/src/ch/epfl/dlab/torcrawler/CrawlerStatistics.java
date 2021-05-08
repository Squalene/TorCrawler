package ch.epfl.dlab.torcrawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.annotations.Expose;

/**
 * @author Antoine Masanet
 *
 *         Singleton class that stores global variables representing the crawl's
 *         current status and stores them to a file. IMPORTANT: the class must be initialised with a queue
 *         and a thread count before being used.
 *         
 *         CAVEAT: crawlTime is no longer valid when the queue is restored
 */
public final class CrawlerStatistics{
	//The use of @Expose is to indicate to gson what should be convert to Json
	@Expose private AtomicInteger pagesCorrectlyFetched;
	@Expose private AtomicInteger pagesFetchError;
	@Expose private AtomicInteger validUrlsDiscovered;
	private RoundRobinBlockingQueue queue;
	@Expose private AtomicInteger[] pagesCorrectlyFetchedPerThread;
	@Expose private AtomicInteger[] pagesFetchErrorPerThread;
	@Expose private int threadCount;
	private long crawlStart;

	private static CrawlerStatistics INSTANCE = null;

	private CrawlerStatistics(RoundRobinBlockingQueue queue, int threadCount) {
		
		this.threadCount = threadCount;
		crawlStart = System.currentTimeMillis();
		pagesFetchError = new AtomicInteger(0);
		validUrlsDiscovered = new AtomicInteger(0);
		pagesCorrectlyFetched = new AtomicInteger(0);
		this.queue = queue;
		pagesCorrectlyFetchedPerThread = new AtomicInteger[threadCount];
		for (int i = 0; i < threadCount; ++i) {
			pagesCorrectlyFetchedPerThread[i] = new AtomicInteger(0);
		}
		pagesFetchErrorPerThread = new AtomicInteger[threadCount];
		for (int i = 0; i < threadCount; ++i) {
			pagesFetchErrorPerThread[i] = new AtomicInteger(0);
		}

	}

	/**
	 * Initializes the singleton crawler statistic object if it has not already been initialized
	 * @param queue: the queue
	 * @param threadCount
	 * @return true if the initialization modified the singleton
	 */
	public synchronized static boolean initialize(RoundRobinBlockingQueue queue, int threadCount) {
		if (queue == null || threadCount < 1) {
			throw new IllegalArgumentException();
		}

		if (INSTANCE != null) {
			return false;
		}

		INSTANCE = new CrawlerStatistics(queue, threadCount);
		return true;
	}
	

	/**
	 * Set the current instance to this instance. IMPORTANT: the instance passed as a parameter should not be shared amongst other classes.
	 * @param stats: the new instance
	 */
	public synchronized static void setInstance (CrawlerStatistics stats) {
		if (stats == null) {
			throw new IllegalArgumentException("Arg hsould not be null");
		} 
		
		INSTANCE = stats;
	}
	
	/**
	 * Set the crawler statistics queue
	 * @param queue
	 */
	public synchronized void setQueue(RoundRobinBlockingQueue queue) { 
		if(queue==null) {
			throw new IllegalArgumentException("The queue should not be null");
		}
		this.queue=queue;
	}
	
	/**
	 * Sets the crawl start time to the current time
	 */
	public void resetCrawlTime() {
		crawlStart = System.currentTimeMillis();
	}
	

	/**
	 * Restore the statistics from the default file path
	 * NOTE: this function works jointly with the toSave function
	 * @param queue: the queue
	 * @param filePath: the path of the recovery file
	 * @return true if the singleton instance has been properly initialized from the file
	 */
	public synchronized static boolean restore(RoundRobinBlockingQueue queue) throws FileNotFoundException{
		return restore(queue,FileUtility.DATA_FOLDER, FileUtility.RECOVERY_FILE_NAME);
	}
	

	/**
	 * Recovers the current status from a file and initialize the singleton crawl status with it
	 * NOTE: this function works jointly with the toSave function
	 * @param queue: the queue
	 * @param filePath: the path of the recovery file
	 * @return true if the singleton instance has been properly initialized from the file
	 */
	public synchronized static boolean restore(RoundRobinBlockingQueue queue, String directory,String fileName) throws FileNotFoundException{

		if (queue == null ||directory==null || fileName == null) {
			throw new IllegalArgumentException("Argument should not be null");
		}
		if (INSTANCE != null) {
			return false;
		}
		
		INSTANCE = FileUtility.loadCrawlerStatistics(directory, fileName);
		INSTANCE.resetCrawlTime();
		INSTANCE.setQueue(queue);
		
		return true;
	}

	/**
	 * @return true if the singleton has already been initialized
	 */
	public synchronized boolean hasBeenInitialized() {
		return INSTANCE != null;
	}

	/**
	 * Returns the current instance
	 * @throws IllegalStateException if the singleton has not already been initialized
	 * @return the current instance
	 */
	
	public synchronized static CrawlerStatistics getInstance() {
		if (INSTANCE == null) {
			throw new IllegalStateException("Singleton has not been initialized");
		}
		return INSTANCE;
	}

	/**
	 * Returns the total number of pages fetched 
	 * @return the total number of pages fetched 
	 */
	public int getTotalPagesFetched() {
		return pagesCorrectlyFetched.get() + pagesFetchError.get();
	}

	/**
	 * Returns the total number of pages that have been correctly fetched
	 * @return the total number of pages that have been correctly fetched
	 */
	public synchronized int getPagesCorrectlyFetched() {
		return pagesCorrectlyFetched.get();
	}

	/**
	 * Returns the total number of pages that have been correctly fetched by the i'th thread
	 * @param i: the threads id
	 * @return the total number of correctly fetched pages
	 */
	public int getPagesCorrectlyFetchedByThread(int i) {
		if (i < 0 || i >= pagesCorrectlyFetchedPerThread.length) {
			throw new IllegalArgumentException();
		}
		return pagesCorrectlyFetchedPerThread[i].get();
	}

	/**
	 * Increment the total number of pages that have been correctly fetched by the i'th thread
	 * @param i: the threads id
	 */
	public void incrementPagesCorrectlyFetchedByThread(int i) {
		if (i < 0 || i >= pagesCorrectlyFetchedPerThread.length) {
			throw new IllegalArgumentException("Thread id out of bound");
		}

		pagesCorrectlyFetchedPerThread[i].incrementAndGet();
		pagesCorrectlyFetched.incrementAndGet();
	}

	/**
	 * Returns the total number of pages that have been incorrectly fetched
	 * @return the total number of pages that have been incorrectly fetched
	 */
	public int getPagesFetchError() {
		return pagesFetchError.get();
	}

	/**
	 * Returns the total number of pages that have been incorrectly fetched by the i'th thread
	 * @param i: the threads id
	 * @return the total number of incorrectly fetched pages
	 */
	public int getPagesFetchErrorByThread(int i) {
		if (i < 0 || i >= pagesFetchErrorPerThread.length) {
			throw new IllegalArgumentException();
		}
		return pagesFetchErrorPerThread[i].get();
	}

	/**
	 * Increment the total number of pages that have been incorrectly fetched by the i'th thread
	 * @param i: the threads id
	 */
	public void incrementPagesFetchErrorByThread(int i) {
		if (i < 0 || i >= pagesFetchErrorPerThread.length) {
			throw new IllegalArgumentException("Thread id out of bound");
		}

		pagesFetchErrorPerThread[i].incrementAndGet();
		pagesFetchError.incrementAndGet();
	}

	/**
	 * Returns the total number of domains discovered
	 * @return the total number of domains discovered
	 */
	public int getDomainsDiscovered() {
		return queue.domainsDiscovered();
	}

	/**
	 * Returns the total number of valid urls discovered
	 * @return the total number of valid urls discovered
	 */
	public int getValidUrlsDiscovered() {
		return validUrlsDiscovered.get();
	}

	/**
	 * Increment the total number of valid urls discovered
	 */
	public void incrementValidUrlsDiscovered() {
		validUrlsDiscovered.incrementAndGet();
	}

	/**
	 * Returns in ms the time elapsed from the beginning of the crawl. This time is
	 * recorded from the moment this class is instantiated.
	 * 
	 * @return the crawl time
	 */
	public double getCrawlTime() {
		return System.currentTimeMillis() - crawlStart;
	}

	/**
	 * Return the fraction of subqueues that are not empty
	 * @return the fraction of subqueues that are not empty
	 */
	public double getSubqueueUtilization() {
		return queue.subqueueUtilization();
	}

	/**
	 * Return the total number of urls in the queue
	 * @return the total number of urls in the queue
	 */
	public int getQueueSize() {
		return queue.size();
	}
	
	/**
	 * Return the number of threads in the crawl
	 * @return the number of threads in the crawl
	 */
	public int getThreadCount() {
		return threadCount;
	}

	@Override
	public String toString() {
		
		if(INSTANCE==null) {
			return "null";
		}
		
		StringBuilder sb = new StringBuilder();

		sb.append("Thread count: "+threadCount + "\n");
		sb.append("Queue size: " + getQueueSize() + "\n");
		sb.append("Pages correctly fetched: " + pagesCorrectlyFetched.get() + "\n");
		sb.append("Pages with fetch error: " + pagesFetchError.get() + "\n");
		sb.append("Domain discovered: " + getDomainsDiscovered() + "\n");
		sb.append("Valid urls discovered: " + validUrlsDiscovered.get() + "\n");
		sb.append("Percentage of non empty subqueues: " + 100 * getSubqueueUtilization() + "%\n");
		for (int i = 0; i < pagesCorrectlyFetchedPerThread.length; ++i) {
			sb.append("Thread " + i + " fetched:" + pagesCorrectlyFetchedPerThread[i].get() + " pages\n");
		}

		sb.append("Crawl time: " + getCrawlTime() / 60_000 + "min\n");//Only valid if not restored
		return sb.toString();
	}

}
