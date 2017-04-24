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

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 
 */
public class Logger {

	public static final String DEFAULT_LOGS_PATH = "logs";
	private static final String LOG_ENTRY_FORMAT = "%s,%s,%s";
	private static final String DATE_ENTRY_FORMAT = "yyyy.MM.dd.HH.mm.ss";
	
	private static String logContents = "";
	private static File log = new File(DEFAULT_LOGS_PATH + "/log.csv");
	
	/**
	 * 
	 * @param message
	 */
	public static void info(String message) {
		String entry = String.format(LOG_ENTRY_FORMAT, "INFO", getCurrentTimestamp(), message);
		logContents += entry + "\n";
	}
	
	/**
	 * 
	 * @param message
	 */
	public static void error(String message) {
		String entry = String.format(LOG_ENTRY_FORMAT, "ERROR", getCurrentTimestamp(), message);
		logContents += entry + "\n";
	}
	
	/**
	 * 
	 * @param message
	 */
	public static void debug(String message) {
		String entry = String.format(LOG_ENTRY_FORMAT, "DEBUG", getCurrentTimestamp(), message);
		logContents += entry + "\n";
	}
	
	/**
	 * 
	 */
	public static void writeResultsToLog() {
		Utils.writeToFile(log, logContents);
	}
	
	/**
	 * 
	 * @return
	 */
	private static String getCurrentTimestamp() {
		Date currentDate = new Date();
		String timestamp = (new SimpleDateFormat(DATE_ENTRY_FORMAT)).format(currentDate);
		return timestamp;
	}

}