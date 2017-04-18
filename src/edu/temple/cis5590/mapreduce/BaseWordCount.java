// =======================================================================
//
//	Course:		CIS 5590, Spring 2017
//	Professor:	X. He
//	
//	Author:		Sarah M. Lehman
//	Email:		smlehman@temple.edu
//
//	Program:	Semester Project, AWS Hadoop Map-Reduce
//  Source:		https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
//
// =======================================================================

package edu.temple.cis5590.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class BaseWordCount {
	
	/**
	 * 
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static String[] targetWords = 
				new String[] { "economy", "education", "government", "sports" };
		private final static IntWritable zero = new IntWritable(0);
		private final static IntWritable one = new IntWritable(1);
		
		private Text word = new Text();
		private boolean init = false;
		
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
			// initialize tokens so that all countries display at least a zero 
			// for each token
			if (!init) {
				initializeTokens(context);
				init = true;
			}
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				// force word to lowercase to cancel any capitalization discrepancies
				String token = itr.nextToken().toLowerCase();
				if (Arrays.asList(targetWords).contains(token)) {
					word.set(getCountryToken(context, token));
					context.write(word, one);
				}
			}
		}
		
		/**
		 * 
		 * @param context
		 */
		private void initializeTokens(Context context)
									  throws IOException, InterruptedException {
			for (int i = 0; i < targetWords.length; i++) {
				word.set(getCountryToken(context, targetWords[i]));
				context.write(word, zero);
			}
		}
		
		/**
		 * 
		 * @param context
		 * @param token
		 * @return
		 */
		private String getCountryToken(Context context, String token) {
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			String countryName = filename.substring(0, filename.indexOf("."));
			return (countryName + " - " + token);
		}
	}
	
	/**
	 * 
	 */
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		/**
		 * 
		 * @param key
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
						  throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
  
}