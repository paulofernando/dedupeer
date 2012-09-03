package net.collegeman.rsync;

public class RsyncException extends RuntimeException {

	private static final long serialVersionUID = 5691153711788823995L;
	
	public RsyncException(String message) {
		super(message);
	}
	
	public RsyncException(String message, Throwable cause) {
		super(message, cause);
	}

}
