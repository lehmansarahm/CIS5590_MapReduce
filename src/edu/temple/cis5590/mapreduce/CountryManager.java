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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CountryManager {

	public final static String[] COUNTRIES = 
			new String[] { "Denmark", "Finland", "France", "Ireland", "Netherlands", 
					"Norway", "Sweden", "Switzerland", "UnitedKingdom", "UnitedStates" };
	
	/**
	 * 
	 * @param context
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static Text getCountryText(Context context) {
		String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
		filename = filename.substring(0, filename.indexOf("."));	// strip extension
		return (new Text(filename));
	}
	
	/**
	 * 
	 * @param context
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static String getCountryToken(Context context) {
		String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
		filename = filename.substring(0, filename.indexOf("."));	// strip extension

		// Log processing of new country token
		// Logger.info("New token found: " + filename);
		return filename;
	}
	
	/**
	 * 
	 * @param context
	 * @param token
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static String getCountryToken(Context context, String token) {
		String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
		filename = filename.substring(0, filename.indexOf("."));	// strip extension
		String newToken = (filename + "-" + token);

		// Log processing of new country token
		// Logger.info("New token found: " + newToken);
		return newToken;
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public static Text getTextForMapKey(Text key) {
		// keys should be coming in using format [file name][dash][target word]
		String[] str = key.toString().split("-");
		Text textVal = (new Text((str.length >= 2) ? str[1] : str[0]));
		
		// Log processing of new text value
		// Logger.info("MapReduce value returned: " + textVal.toString());
		return textVal;
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public static int getPartitionForMapKey(CountryTokenKey key) {
		// keys should be coming in using format [file name][dash][target word]
		String countryName = key.getCountry().toString();
		int partition = getPartitionForCountryName(countryName);

		Logger.info("Country: " + countryName 
				+ " with token: " + key.getToken()
				+ " assigned to partition: " + partition);
		return partition;
	}
	
	/**
	 * 
	 * @param countryName
	 * @return
	 */
	public static int getPartitionForCountryName(String countryName) {
		for (int i = 0; i < COUNTRIES.length; i++) {
			if (COUNTRIES[i].equalsIgnoreCase(countryName)) {
				return i;
			}
		}
		
		// No matches found ... add to log
		Logger.error("No partition matches found for country: " + countryName);
		return 0;
	}
	
}