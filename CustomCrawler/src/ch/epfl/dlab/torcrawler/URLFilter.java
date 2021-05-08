package ch.epfl.dlab.torcrawler;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Antoine Masanet
 * 
 * Static class that provides utility functions to filter the urls
 *
 */
public final class URLFilter {
	
	/**
	 * Returns true if the url is to be kept with respect to the crawlers objective
	 * @param url
	 * @return whether the url is to be kept
	 */
	public static boolean filterURL(String url) {
		if(url==null) {
			return false; 
		}
		String domain = Fetcher.getDomain(url);
		return domain!=null && domain.contains(".onion") && regexKeep(url) && isUrlValid(url);
	}
	
	
	
	/**
	 * Verifies if the url provided is valid 
	 * @param url: the provided url
	 * @return whether the url is valid
	 */
	public static boolean isUrlValid(String url) {
		try {
			URL obj = new URL(url);
			obj.toURI();
			return true;
			
		} catch (MalformedURLException e) {
			System.out.println("Url:" +url+ "is malformed");
			return false;
		} catch (URISyntaxException e) {
			System.out.println("Url:" +url+ "URISyntaxException");
			return false;
		}
	}
	
	/**
	 * Returns true if the url is to be kept according to regex rules 
	 * @param url: a string representing the url
	 * @return whether to keep the url or not 
	 */
	public static boolean regexKeep(String url) {
		
		//skip "file:" "ftp:" and "mailto:" urls=>Not needed because enforce it to start by http or https
		//String regexExclude = "^(file|ftp|mailto):"; 
		
		//skip image and other suffixes we can't parse or are not likely to be relevant
		String regexExclude ="(?i)\\.(apk|deb|cab|iso|gif|jpg|png|svg|ico|css|sit|eps|wmf|rar|tar|jar|zip"
				+ "|gz|bz2|rpm|tgz|mov|exe|jpeg|jpe|bmp|js|mpg|mp3|mp4|m4a|ogv|kml|wmv|swf|flv"
				+ "|mkv|m4v|webm|ra|wma|wav|avi|xspf|m3u)(\\?|&|$)";
		
		//exclude localhost and loop-back addresses
		regexExclude+="|^https?://(?:localhost|127(?:\\.(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))){3}|\\[::1\\])(?::\\d+)?(?:/|$)";
		
		/*
		 
		//exclude private IP address spaces 10.0.0.0/8
		regex+="|^https?://(?:10(?:\\.(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))){3})(?::\\d+)?(?:/|$)";
		
		//exclude private IP address spaces 192.168.0.0/16
		regex+="|^https?://(?:192\\.168(?:\\.(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))){2})(?::\\d+)?(?:/|$)";
		
		//exclude private IP address spaces 172.16.0.0/12
		regex+="|^https?://(?:172\\.(?:1[6789]|2[0-9]|3[01])(?:\\.(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))){2})(?::\\d+)?(?:/|$)";
		
		*/
		
		Pattern patternExclude = Pattern.compile(regexExclude, Pattern.CASE_INSENSITIVE);
		 
		Matcher matcherExclude = patternExclude.matcher(url);
		
		String regexInclude =  "^(http|https)://";//Only keep http and https protocol
		
		Pattern patternInclude = Pattern.compile(regexInclude,Pattern.CASE_INSENSITIVE);
		
		Matcher matcherInclude = patternInclude.matcher(url);
		
		return !matcherExclude.find() && matcherInclude.find();
		 
	}
}
