package ch.epfl.dlab.torcrawler;

/**
 * @author Antoine Masanet
 * Represents a url with a status, and the url that lead to it
 * NOTE: not used in the current implementation
 */
public final class EnhancedURL {

	public enum Status{DISCOVERED,FETCHED,FETCH_ERROR}
	
	private String url;
	private Status status;
	private String parentURL;//Reference to parent
	
	public EnhancedURL(String url, Status status, String parentURL) {
		this.url=url;
		this.status=status;
		this.parentURL=parentURL;
	}
	
	public String getURLString() {
		return url;
	}
	
	public String getParentURL() {
		return parentURL;
	}
	
	public Status getStatus() {
		return status;
	}
	
	public void setStatus(Status status) {
		this.status=status;
	}
	
}
