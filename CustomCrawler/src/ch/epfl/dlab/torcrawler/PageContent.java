package ch.epfl.dlab.torcrawler;


import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Antoine Masanet
 *
 * Plain Old Java Object (POJO) that stores the content of a page. 
 * Each PageContent is immutable.
 */
public final class PageContent {

	public final String pageUrl;
	public final String content;
	public final Set<String> linkURLs;
	public final String title;
	
	public static int MAX_CONTENT_LENGTH = 65_536;
	public static int MAX_CONTENT_PRINT = 1024;
	
	
	/**
	 * Creates a new Page Content object, if the content is too long, it will be truncated to MAX_CONTENT_LENGTH
	 * @param parentURL
	 * @param content
	 * @param URLs
	 */
	public PageContent(String pageUrl,String title, String content,  Set<String> linkURLs) {
		
		this.pageUrl = pageUrl;
		this.content=content.substring(0, Math.min(content.length(), MAX_CONTENT_LENGTH));
		this.linkURLs = Collections.unmodifiableSet(new HashSet<>(linkURLs));	
		this.title=title;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("pageUrl: "+pageUrl).append("\n");
		sb.append("Title: "+title).append("\n");
		sb.append("content: "+content.substring(0, Math.min(content.length(), MAX_CONTENT_PRINT))).append("\n");
		sb.append("URLs: "+linkURLs.toString()).append("\n");
		
		return sb.toString();
	}
}
