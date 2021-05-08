/**
 * 
 */
package ch.epfl.dlab.torcrawler;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Static class that provides utility functions to connect to websites and fetch
 * their pages
 */

public final class Fetcher {

	private static final int PROXY_PORT = 8118;
	// Correspond to the user agent of the Tor browser
	private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; rv:78.0) Gecko/20100101 Firefox/78.0";
	private static final int TIMEOUT = 100_000;// in ms

	private Fetcher() {
	}

	/**
	 * Fetches the page corresponding to this url
	 * 
	 * @param url: the url of the page, must be absolute
	 * @return the page content
	 */
	public static PageContent fetchPage(String url,Map<String,Map<String,String>> cookies) {
		if(cookies == null || url==null) {
			throw new IllegalArgumentException("The arguments should not be null");
		}
		
		PageContent pageContent = null;
		Document doc = null;
		
		try {
			doc = connect(url,cookies);
		} catch (IOException e) {
			System.out.println("Could not fetch page with url:" + url);
			e.printStackTrace();
			return null;
		}

		if (doc != null) {
			Set<String> links = new HashSet<>();
			String title = doc.title();
			String content = "";
			if(doc.body()!=null) {
				content = doc.body().text();
			}
			Elements elements = doc.select("a[href]");

			for (Element element : elements) {
				links.add(normalizeURL(element.absUrl("href")));
			}

			pageContent = new PageContent(url, title, content, links);
		}

		return pageContent;
	}


	/**
	 * Returns all the links of the provided page
	 * @param url: the urls to fetch the page from
	 * @return the set of links of the page
	 */
	public static Set<String> fetchLinks(String url,Map<String,Map<String,String>> cookies) {

		if(cookies == null || url==null) {
			throw new IllegalArgumentException("The arguments should not be null");
		}
		
		Document doc=null;
		try {
			doc = connect(url,cookies);
		} catch (IOException e) {
			System.out.println("Could not fetch page with url:" + url);
			e.printStackTrace();
			return null;
		}
		 	Set<String> links=null;
		 	
			if(doc!=null) {
				Elements elements = doc.select("a[href]");
				links = new HashSet<>();

				for (Element element : elements) {
					links.add(normalizeURL(element.absUrl("href")));
				}
			}
			
		return links;
	}

	/**
	 * Removes extra border space of the url and replace intra space with corresponding character (%20)
	 * @param url: the url to normalize
	 * @return the normalized url
	 */
	private static String normalizeURL(String url) {

		return url.trim().replaceAll("\\s", "%20");// Remove extra white space+ replace space with %20
	}

	/**
	 * Create a document corresponding to the connection to the given url using a cookies if available
	 * @param url: the url
	 * @param cookies: a map containing all the available cookies
	 * @return a Jsoup document 
	 * @throws IOException
	 */
	private static Document connect(String url, Map<String,Map<String,String>> cookies) throws IOException{

		 Connection connection = Jsoup.connect(url).proxy("localhost", PROXY_PORT).timeout(TIMEOUT)
				.header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
				.header("Accept-Encoding", "gzip, deflate").header("Accept-Language", "en-US,en;q=0.5")
				.header("Cache-Control", "max-age=0").header("Connection", "keep-alive").userAgent(USER_AGENT)
				.header("Upgrade-Insecure-Requests", "1");
		 
		 String domain = getDomain(url);
			Map<String,String> domainCookies = null;
			if(domain!=null) {
				domainCookies = cookies.get(domain);
				if(domainCookies!=null) {
					connection = connection.cookies(domainCookies);
				}
			}
			
			
		
		return connection.get();
	}

	/**
	 * Returns the domain of the url or null if the domain could not be determined
	 * @param url
	 * @return the domain of the url
	 */
	public static String getDomain(String url) {

		String domain = null;

		try {
			domain = new URI(url).getHost();
			if(domain==null) {
				return null;
			}
			domain = domain.toLowerCase();
			if(domain.matches("^[a-z0-9.-]*$")) {
				return domain.startsWith("www.") ? domain.substring(4) : domain;
			}
			else {
				System.out.println("Domain is incorrect:"+domain);
				return null;
			}
			
		} catch (URISyntaxException e) {
			System.out.println("Could not extract domain from url:" + url);
		}
		return null;
	}
	

	/**
	 * Adds .pet at the end of the url for the query to go throught the pet proxy (not used in this project)
	 * @param url
	 * @return the modified url
	 */
	public static String addPetProxy(String url) {
		return url.replaceFirst(".onion", ".onion.pet");
	}
}
