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
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 */
public class BaseWordCount {

	// ============================================================================================
	//										MAP
	// ============================================================================================
	
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
				
				// it's possible that the tokenizer won't parse out the target words 
				// completely (lingering punctuation, squished-together words, etc.)  
				// To address this, search the token for each target word, instead 
				// of the array of target words for each token
				for (int i = 0; i < targetWords.length; i++) {
					if (token.contains(targetWords[i])) {
						// increment target word count
						word.set(CountryManager.getCountryToken(context, targetWords[i]));
						context.write(word, one);
						
						// increment total word count
						word.set(CountryManager.getCountryToken(context));
						context.write(word, one);
						
						// exit loop
						break;
					}
				}
			}
		}
		
		/**
		 * 
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		private void initializeTokens(Context context)
									  throws IOException, InterruptedException {
			for (int i = 0; i < targetWords.length; i++) {
				word.set(CountryManager.getCountryToken(context, targetWords[i]));
				context.write(word, zero);
			}
		}
	}

	// ============================================================================================
	//										REDUCE
	// ============================================================================================
	
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
			for (IntWritable val : values) sum += val.get(); 
			result.set(sum);
			context.write(key, result);
		}
	}
  
}