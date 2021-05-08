package ch.epfl.dlab.torcrawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

/**
 * @author Antoine Masanet
 * Static class that provides utility functions to fetch/save maps,sets,crawlerStatistics from/to files
 */
public final class FileUtility {
	
	public static final String DATA_FOLDER = "data";
	public static final String DISCOVERED_URLS_FILE_NAME = "discoveredUrls.json";
	public static final String RECOVERY_FILE_NAME = "crawlStatus.json";
	public final static String RESOURCES_FOLDER = "src/resources";
	public final static String SEEDS_FILE = "seeds.json";
	public final static String COOKIES_FILE = "cookies.json";
	
	
	private FileUtility() {}
	
	/**
	 * Returns the set of url stored in the seed file.
	 * @throws FileNotFoundException if the file cannot be found
	 * @return the set of urls stored in the seed file
	 */
	
	public static Set<String> fetchSeedURLs() throws FileNotFoundException{
		return loadSet(RESOURCES_FOLDER, SEEDS_FILE); 
	}
	
	/**
	 * Returns the set of url stored in the discovered url file of a previous crawl.
	 * @throws FileNotFoundException if the file cannot be found
	 * @return the set of urls stored in the file
	 */
	public static Set<String> fetchDiscoveredURLs() throws FileNotFoundException{
		return loadSet(DATA_FOLDER, DISCOVERED_URLS_FILE_NAME); 
	}
	
	/**
	 * Stores in the discovered url file this set of urls (will overwrite the file if it exists)
	 * @param discoveredUrls : the set of urls to store
	 * @throws IOException
	 */
	public static void saveDiscoveredURLs(Set<String> discoveredUrls) throws IOException{
		saveSet(DATA_FOLDER, DISCOVERED_URLS_FILE_NAME, discoveredUrls);
	}
	
	/**
	 * Returns the map of cookies stored in the cookies file.
	 * @throws FileNotFoundException if the file cannot be found
	 * @return the map of cookies stored in the file, the map is Map<domain,Map<cookieName,cookieContent>>
	 */
	public static Map<String,Map<String,String>> fetchCookies() throws FileNotFoundException {
		return loadMap(RESOURCES_FOLDER, COOKIES_FILE);
	}
	
	/**
	 * Saves the map of cookies to the cookie file
	 * @param cookies: the map of cookies to save
	 * @throws IOException
	 */
	public void saveCookies(Map<String,Map<String,String>> cookies) throws IOException {
		saveMap(RESOURCES_FOLDER, COOKIES_FILE,cookies);
	}
	
	/**
	 * Save a generic map to a file (will overwrite the file if it exists)
	 * @param <K>: the key parameter of the map
	 * @param <V>: the value parameter of the map
	 * @param directory: the name of the directory where the map will be saved
	 * @param fileName: the name of the file where the map will be saved
	 * @param map: the map to save
	 * @throws IOException
	 */
	public static <K,V> void saveMap(String directory,String fileName, Map<K,V> map) throws IOException{
		
		File file = new File(directory,fileName);
		BufferedWriter writer = new BufferedWriter(new FileWriter(file,false));//Overwrite the file content if exists
		String json = new Gson().toJson(map);
		writer.write(json);
		writer.close();
	}
	
	/**
	 * Loads a generic map from a file
	 * @param <K>: the key parameter of the map
	 * @param <V>: the value parameter of the map
	 * @param directory: the name of the directory from which the map will be loaded
	 * @param fileName: the name of the file from which the map will be loaded
	 * @return the map obtained from the file
	 * @throws FileNotFoundException if the file cannot be found
	 */
	public static <K,V> Map<K,V> loadMap(String directory,String fileName) throws FileNotFoundException{
		File file = new File(directory,fileName);
		Type type = new TypeToken<Map<K, V>>() {}.getType();
	
		JsonReader reader = new JsonReader(new InputStreamReader(new FileInputStream(file)));	
		Map <K,V> map = new Gson().fromJson(reader,type);
		return map;
	}
	
	/**
	 * Saves a generic set to the provided file (will overwrite the file if it exists)
	 * @param <E>: the type of the element stored in the file
	 * @param directory: the name of the directory where the set will be saved
	 * @param fileName: the name of the file where the set will be saved
	 * @param set: the set to save
	 * @throws IOException
	 */
	public static <E> void saveSet(String directory,String fileName,Set<E> set) throws IOException{
		
		File file = new File(directory,fileName);
		BufferedWriter writer = new BufferedWriter(new FileWriter(file,false));//Do not append
		String json = new Gson().toJson(set);
		writer.write(json);
		writer.close();
	}
	
	/**
	 * Loads a generic set from a file
	 * @param <E>: the type of the element stored in the file
	 * @param directory: the name of the directory from which the set will be loaded
	 * @param fileName: the name of the file from which the set will be loaded
	 * @return the set stored in the given file
	 * @throws FileNotFoundException: if the file cannot be found
	 */
	public static <E> Set<E> loadSet(String directory,String fileName) throws FileNotFoundException{
		File file = new File(directory,fileName);
		Type type = new TypeToken<Set<E>>() {}.getType();
		JsonReader reader = new JsonReader(new InputStreamReader(new FileInputStream(file)));	
		Set <E> map = new Gson().fromJson(reader,type);
		return map;
	}
	
	/**
	 * @param directory: the name of the directory where the crawlerStatistics will be saved
	 * @param fileName: the name of the file where the crawlerStatistics will be saved
	 * @param stats the crawlerStatistics to save
	 * @throws IOException
	 */
	public static void saveCrawlerStatistics(String directory,String fileName, CrawlerStatistics stats) throws IOException{
		
		File file = new File(directory,fileName);
		BufferedWriter writer = new BufferedWriter(new FileWriter(file,false));//Do not append
		Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
		String json = gson.toJson(stats);
		writer.write(json);
		writer.close();
	}
	
	/**
	 * Saves the crawlerStatistics in the default file
	 * @throws IOException
	 */
	public static void saveCrawlerStatistics() throws IOException{
		saveCrawlerStatistics(DATA_FOLDER,RECOVERY_FILE_NAME,CrawlerStatistics.getInstance());
	}
	
	/**
	 * Returns the crawlerStatistics obtained from the file
	 * @param directory: the name of the directory from which the crawlerStatistics will be loaded
	 * @param fileName: the name of the file from which the crawlerStatistics will be loaded
	 * @return the crawlerStatistics obtained from the file
	 * @throws FileNotFoundException if the file cannot be found
	 */
	public static CrawlerStatistics loadCrawlerStatistics(String directory,String fileName) throws FileNotFoundException {
		
		File file = new File(directory,fileName);
		JsonReader reader = new JsonReader(new InputStreamReader(new FileInputStream(file)));
		CrawlerStatistics stats = new Gson().fromJson(reader,CrawlerStatistics.class);
		return stats;
	}
	
	/**
	 * Loads the crawlerStatistics loaded from the default file
	 * @return the crawlerStatistics
	 * @throws FileNotFoundException if the file cannot be found
	 */
	public static CrawlerStatistics loadCrawlerStatistics() throws FileNotFoundException {
		return loadCrawlerStatistics(DATA_FOLDER, RECOVERY_FILE_NAME);
	}
}
