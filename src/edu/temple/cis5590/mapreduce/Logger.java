// =======================================================================
//
//	Course:		CIS 5590, Spring 2017
//	Professor:	X. He
//	
//	Author:		Sarah M. Lehman
//	Email:		smlehman@temple.edu
//
//	Program:	Semester Project, AWS Hadoop Map-Reduce
//
// =======================================================================

package edu.temple.cis5590.mapreduce;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;

/**
 * Utility class to write execution feedback to file
 */
public class Logger {

	public static final String DEFAULT_LOGS_PATH = "logs";
	private static final String LOG_ENTRY_FORMAT = "%s,%s,%s";
	private static final String DATE_ENTRY_FORMAT = "yyyy.MM.dd.HH.mm.ss";
	
	private static String logContents = "";
	private static String logFilename = (DEFAULT_LOGS_PATH + "/log.csv");
	
	/**
	 * Writes an "info" class message to the log
	 * @param message - the message to add to log
	 */
	public static void info(String message) {
		String entry = String.format(LOG_ENTRY_FORMAT, "INFO", getCurrentTimestamp(), message);
		logContents += entry + "\n";
	}

	/**
	 * Writes an "error" class message to the log
	 * @param message - the message to add to log
	 */
	public static void error(String message) {
		String entry = String.format(LOG_ENTRY_FORMAT, "ERROR", getCurrentTimestamp(), message);
		logContents += entry + "\n";
	}

	/**
	 * Writes a "debug" class message to the log
	 * @param message - the message to add to log
	 */
	public static void debug(String message) {
		String entry = String.format(LOG_ENTRY_FORMAT, "DEBUG", getCurrentTimestamp(), message);
		logContents += entry + "\n";
	}
	
	/**
	 * Dumps the current set of log messages to the log file
	 * @param conf - the configuration to use when writing the log contents
	 */
	public static void writeResultsToLog(Configuration conf) {
		Utils.writeToFile(conf, logFilename, logContents);
	}
	
	/**
	 * Returns the current date-time stamp
	 * @return - the current date-time stamp
	 */
	private static String getCurrentTimestamp() {
		Date currentDate = new Date();
		String timestamp = (new SimpleDateFormat(DATE_ENTRY_FORMAT)).format(currentDate);
		return timestamp;
	}

}