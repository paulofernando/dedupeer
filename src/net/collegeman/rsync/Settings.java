package net.collegeman.rsync;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class Settings {

	private String src;
	
	private String dest;
	
	private Boolean verbose;
	
	private Boolean quiet;
	
	private static Options options = new Options();
	
	static {
		options.addOption(
			OptionBuilder.withArgName("path")
				.hasArg()
				.withDescription("path to local source")
				.create("src")
		);
	
		options.addOption(
			OptionBuilder.withArgName("path")
				.hasArg()
				.withDescription("path to local/remote destination")
				.create("dest")
		);
	
		options.addOption("delete", false, "delete extraneous files from destination directory");
		
		options.addOption("v", "verbose", false, "increase verbosity");
		options.addOption("q", "quiet", false, "suppress non-error messages");
		
		options.addOption("h", "help", false, "prints this message");
	}
	
	public static Options getOptions() {
		return options;
	}
	
	public Settings() {}
	
	public Settings(String[] args) {
		try {
			CommandLineParser parser = new PosixParser();
			CommandLine cmd = parser.parse(options, args);
			
			if (cmd.hasOption("help") || cmd.hasOption("?"))
				throw new RsyncHelpException();
			
			setSrc(cmd.getOptionValue("src"));
			setDest(cmd.getOptionValue("dest"));
			setVerbose(cmd.hasOption("v") || cmd.hasOption("verbose"));
			setQuiet(cmd.hasOption("q") || cmd.hasOption("quiet"));
			
		} catch (ParseException e) {
			throw new IllegalArgumentException("Failed to parse command line arguments", e);
		}
	}

	public String getSrc() {
		return src;
	}

	public void setSrc(String src) {
		this.src = src;
	}

	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
	}

	public Boolean getVerbose() {
		return verbose;
	}

	public void setVerbose(Boolean verbose) {
		this.verbose = verbose;
	}

	public Boolean getQuiet() {
		return quiet;
	}

	public void setQuiet(Boolean quiet) {
		this.quiet = quiet;
	}

	

	
}
