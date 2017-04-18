// =======================================================================
//
//	Course:		CIS 5590, Spring 2017
//	Professor:	X. He
//	
//	Author:		Sarah M. Lehman
//	Email:		smlehman@temple.edu
//
//	Program:	Semester Project, AWS Hadoop Map-Reduce
//  Sources:	https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
//				https://www.tutorialspoint.com/map_reduce/map_reduce_partitioner.htm
//
// =======================================================================

package edu.temple.cis5590.mapreduce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
	 * @param entry
	 */
	public static void writeResultsToLog() {
        try {
            if (!log.exists()) log.createNewFile();
            FileOutputStream fos = new FileOutputStream(log);
            OutputStreamWriter osw = new OutputStreamWriter(fos);
            osw.append(logContents);

            osw.close();
            fos.flush();
            fos.close();
        } catch (IOException e) {
            System.out.println("File write failed: " + e.toString());
        }
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