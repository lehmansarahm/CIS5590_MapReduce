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
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 */
public class PopularWordCount {

	// ============================================================================================
	//										MAP
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
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
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				// force word to lowercase to cancel any capitalization discrepancies
				String token = itr.nextToken().toLowerCase();
				if (token.length() >= 5) {
					// increment token word count
					word.set(CountryManager.getCountryToken(context, token));
					context.write(word, one);
					
					// increment total word count
					word.set(CountryManager.getCountryName(context));
					context.write(word, one);
				}
			}
		}
	}
  
}