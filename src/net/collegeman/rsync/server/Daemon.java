package net.collegeman.rsync.server;

import net.collegeman.rsync.Settings;

import org.apache.log4j.Logger;

/**
 * Rsync daemon. 
 * 
 * <p>By default, runs on port 837.</p>
 * 
 * @author Aaron Collegeman aaron@collegeman.net
 * @version 1.0.0
 */
public class Daemon {

	private static final Logger logger = Logger.getLogger(Daemon.class);
	
	private Settings settings;
	
	public Daemon(Settings settings) {
		this.settings = settings;
	}
	
	private static final void debug(Object obj) {
		if (logger.isDebugEnabled())
			logger.debug(obj);
	}
	
	private static final void info(Object obj) {
		if (logger.isInfoEnabled())
			logger.info(obj);
	}
	
	private static final void error(Object obj) {
		logger.error(obj);
	}
	
	

}
