package ch.epfl.dlab.torcrawler.test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import ch.epfl.dlab.torcrawler.CrawlerStatistics;
import ch.epfl.dlab.torcrawler.FileUtility;
import ch.epfl.dlab.torcrawler.RoundRobinBlockingQueue;

class FileUtilityTest {
	
	private final static String TEST_FILE = "./src/ch/epfl/dlab/torcrawler/test/resources/";

	@Test
	void mapCanBeSavedAndReloadedProperly() {
		Map<String,String> savedMap = new HashMap<>();
		
		savedMap.put("Hello", "world");
		savedMap.put("a", "b");
		
		System.out.println(savedMap);
		try {
			FileUtility.saveMap(TEST_FILE, "map.json", savedMap);
		} catch (IOException e) {
			System.err.println("Cannot save map");
			e.printStackTrace();
		}
		
		Map<String, String> loadedMap=null;
		try {
			loadedMap = FileUtility.loadMap(TEST_FILE, "map.json");
		} catch (FileNotFoundException e) {
			System.err.println("CAnnot load map");
		}
		System.out.println(loadedMap);
		assertEquals(savedMap, loadedMap);
	}
	
	@Test
	void cookiesCanBeSavedAndReloadedProperly() {
		
		String COOKIES_FILE = "cookies.json";
		
		Map<String,Map<String,String>> savedCookies = new HashMap<>();
		Map<String,String> map1 = new HashMap<>();
		map1.put("a", "1");
		map1.put("b", "2");
		
		Map<String,String> map2 = new HashMap<>();
		map2.put("z", "1");
		map2.put("y", "2");
		
		savedCookies.put("domain1", map1);
		savedCookies.put("domain2", map1);
		
		System.out.println(savedCookies);
		
		try {
			FileUtility.saveMap(TEST_FILE, COOKIES_FILE, savedCookies);
		} catch (IOException e) {
			System.err.println("Cannot save map");
			e.printStackTrace();
		}
		
		Map<String,Map<String,String>> loadedCookies = null;
		
		try {
			loadedCookies = FileUtility.loadMap(TEST_FILE, COOKIES_FILE);
		} catch (FileNotFoundException e) {
			System.err.println("Cannot load map");
		}
		
		System.out.println(loadedCookies);
		assertEquals(savedCookies, loadedCookies);
	}
	
	@Test
	void setCanBeSavedAndReloadedProperly() {
		Set<String> savedSet = new HashSet<>();
		
		savedSet.add("Hello");
		savedSet.add("World");
		
		System.out.println(savedSet);
		try {
			FileUtility.saveSet(TEST_FILE, "set.json", savedSet);
		} catch (IOException e) {
			System.err.println("Cannot save set");
			e.printStackTrace();
		}
		
		Set<String> loadedMap=null;
		try {
			loadedMap = FileUtility.loadSet(TEST_FILE, "set.json");
		} catch (FileNotFoundException e) {
			System.err.println("Cannot load set");
			e.printStackTrace();
		}
		System.out.println(loadedMap);
		assertEquals(savedSet, loadedMap);
	}
	
	//Normal that tests fail because crawl time is not the same
	@Test
	void crawlerStatisticsCanBeSavedAndReloadedProperly() {
		
		String directory = TEST_FILE;
		String fileName = "crawlerStats.json";
		
		RoundRobinBlockingQueue queue = RoundRobinBlockingQueue.create(TEST_FILE +"testQueue");
		
		if(!CrawlerStatistics.initialize(queue, 10)) {
			System.err.println("Queue could not be initilailzed ");
		}
		
		CrawlerStatistics savedInstance = CrawlerStatistics.getInstance();
		
		System.out.println(CrawlerStatistics.getInstance());
		
		try {
			FileUtility.saveCrawlerStatistics(directory, fileName, CrawlerStatistics.getInstance());
		} catch (IOException e) {
			System.err.println("Cannot save crawler statistics");
			e.printStackTrace();
		}
		
		CrawlerStatistics loadedInstance=null;
		try {
			loadedInstance = FileUtility.loadCrawlerStatistics(directory, fileName);
		} catch (FileNotFoundException e) {
			System.err.println("Cannot load crawler statistics");
			e.printStackTrace();
		}
		
		loadedInstance.setQueue(queue);
		loadedInstance.resetCrawlTime();
		
		System.out.println(loadedInstance);
		
		assertEquals(savedInstance, loadedInstance);//Works except for crawl time
		
		try {
			CrawlerStatistics.restore(queue, directory, fileName);
		} catch (FileNotFoundException e) {
			System.err.println("Crawler statistics restore file could  not be found");
			e.printStackTrace();
		}
		
		assertEquals(savedInstance,CrawlerStatistics.getInstance());
	}
}
