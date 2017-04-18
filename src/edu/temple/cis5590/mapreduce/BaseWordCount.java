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

public class BaseWordCount {

	/**
	 * 
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static String[] targetWords = 
				new String[] { "economy", "education", "government", "sports" };
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
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				// force word to lowercase to cancel any capitalization discrepancies
				String token = itr.nextToken().toLowerCase();
				if (Arrays.asList(targetWords).contains(token)) {
					word.set(token);
					context.write(word, one);
				}
			}
		}
	}
	
	/**
	 * 
	 */
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		/**
		 * 
		 * @param key
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
  
}