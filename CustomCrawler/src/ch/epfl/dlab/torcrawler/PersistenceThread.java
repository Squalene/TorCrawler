package ch.epfl.dlab.torcrawler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;

/**
 * @author Antoine Masanet
 * 
 *         A thread that persists the crawler's status to a file CAVEATS: - the
 *         behaviour or status of the file is not defined if the program crashes
 *         during a write (highly unlikely) - no locks on main queue are used
 *         for this thread so the data may be slightly inconsistent
 *
 */
public final class PersistenceThread extends Thread {

	public static final int TIMEOUT = 10_000;// Timeout between each file write of status in ms
	public static final String STATUS_FILE_NAME = "crawlStatus.txt";
	
	private File statusFile;// File used to monitor the crawl
	private Set<String> discoveredUrls;

	public PersistenceThread(Set<String> discoveredUrls) {
		statusFile = new File(FileUtility.DATA_FOLDER, STATUS_FILE_NAME);
		this.discoveredUrls = discoveredUrls;
	}

	@Override
	public void run() {
		try {

			while (true) {
				FileWriter statusFileWriter = null;

				try {
					//Used to have a visualization of the crawler actual state
					statusFileWriter = new FileWriter(statusFile, false);// Overwrites the data
					statusFileWriter.write(CrawlerStatistics.getInstance().toString());

					FileUtility.saveCrawlerStatistics();
					FileUtility.saveDiscoveredURLs(discoveredUrls);//File might get big
						

				} catch (IOException e1) {
					System.err.println("Could not create FileWriter");
					e1.printStackTrace();
				} finally {
					if (statusFileWriter != null) {
						try {
							statusFileWriter.close();
						} catch (IOException e) {
							System.err.println("Could not close fileWriter");
							e.printStackTrace();
						}
					}
				}
				try {
					Thread.sleep(TIMEOUT);
				} catch (InterruptedException e) {
					System.err.println("StatusPersistenceThread interrupted");
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			System.err.println("An unexpected error has occurred:");
			e.printStackTrace();
			return;
		}
	}
}
