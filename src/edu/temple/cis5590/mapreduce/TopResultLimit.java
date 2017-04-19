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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 */
public class TopResultLimit {

	// ============================================================================================
	//										MAP
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class TopResultTokenMapper extends Mapper<Object, Text, CountryTokenKey, IntWritable> {
		
		private Map<String,Integer> countryCounts = new HashMap<String,Integer>();
		
		/**
		 * 
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(Object key, Text value, Context context) 
					   throws IOException, InterruptedException {
			String[] lineValues = value.toString().split("\t");
			String[] keyValues = lineValues[0].split("-");
			CountryTokenKey ctk = new CountryTokenKey();
			
			String countryName = keyValues[0];
			if (!countryCounts.containsKey(countryName)) {
				// initialize count for country name
				countryCounts.put(countryName, 0);
			}
			
			int countryCount = countryCounts.get(countryName);
			if (countryCount < 4) {
				// insert new record
				String token = (keyValues.length >= 2) ? keyValues[1] : keyValues[0];
				int count = (lineValues.length >= 2) ? Integer.parseInt(lineValues[1]) : 0;
				ctk.setProps(countryName, token, count);
				
				// Add country result to final context
				Logger.info("Map phase,Using top result for:" + ctk.toLongString());
				context.write(ctk, ctk.getCount());
				
				// increment count for country name
				countryCount++;
				countryCounts.put(countryName, countryCount);
			}
		}
		
	}
  
}