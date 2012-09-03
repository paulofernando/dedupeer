package net.collegeman.rsync.client;

import net.collegeman.rsync.RsyncException;
import net.collegeman.rsync.RsyncHelpException;
import net.collegeman.rsync.Settings;

import org.apache.commons.cli.HelpFormatter;

/**
 * Command line interface for rsync client.
 * 
 * @author Aaron Collegeman aaron@collegeman.net
 * @since 1.0.0
 */
public final class CLI {
	
	public static void main(String[] args) {
		try {
			new Client(new Settings(args)).start();
		} catch (IllegalArgumentException e) {
			System.err.println(e.getMessage());
		} catch (RsyncHelpException e) {
			help();
		} catch (RsyncException e) {
			System.err.println(e.getMessage());
		}
	}
	
	public static void help() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("rsync", Settings.getOptions());
	}
	
}
