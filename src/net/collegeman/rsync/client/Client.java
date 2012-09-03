package net.collegeman.rsync.client;

import java.util.regex.Pattern;

import net.collegeman.rsync.RsyncException;
import net.collegeman.rsync.Settings;
import net.collegeman.rsync.server.Daemon;

public class Client {

	private Settings settings;
	
	/** For local-to-local comparisons */
	private Daemon daemon;
	
	// ssh connection string: [user@]host:dest
	private Pattern ssh = Pattern.compile("([a-z0-9\\-_]+@)?[a-z0-9\\-_]+:[a-z0-9\\-_ ]+", Pattern.CASE_INSENSITIVE);
	
	// http(s) connection string: http(s)://[user@]host[:port]/dest
	private Pattern http = Pattern.compile("https?://([a-z0-9\\-_]+@)?([a-z0-9\\-_]+\\.)+[a-z]{2,}(:[0-9]+)?/(a-z0-9\\-_ /?)+");
	
	public Client(Settings settings) {
		this.settings = settings;
		
		setupDefault();
		
		if (ssh.matcher(settings.getDest()).matches())
			setupSSH();
		else if (http.matcher(settings.getDest()).matches())
			setupHTTP();
		else
			setupLocal();
	}
	
	/**
	 * Initiates rsync process - blocks until complete.
	 */
	public final void start() {
		
	}
	
	private void assertTrue(boolean state, String message) {
		if (!state)
			throw new RsyncException(message);
	}
	
	private void setupDefault() {
		
	}
	
	private void setupSSH() {
		
	}
	
	private void setupHTTP() {
		
	}
	
	private void setupLocal() {
		// create new local daemon
		daemon = new Daemon(settings);
	}
	
}
