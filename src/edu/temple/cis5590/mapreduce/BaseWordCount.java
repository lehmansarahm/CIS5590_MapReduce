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
public class BaseWordCount {

	// ============================================================================================
	//										MAP
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static String[] TARGET_WORDS = 
				new String[] { "economy", "education", "government", "sports" };
		
		private final static IntWritable ZERO = new IntWritable(0);
		private final static IntWritable ONE = new IntWritable(1);
		
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
				for (int i = 0; i < TARGET_WORDS.length; i++) {
					if (token.contains(TARGET_WORDS[i])) {
						// increment target word count
						word.set(CountryManager.getCountryToken(context, TARGET_WORDS[i]));
						context.write(word, ONE);
						
						// increment total word count
						word.set(CountryManager.getCountryToken(context, CountryTokenKey.TOTAL_TOKEN));
						context.write(word, ONE);
						
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
			for (int i = 0; i < TARGET_WORDS.length; i++) {
				word.set(CountryManager.getCountryToken(context, TARGET_WORDS[i]));
				context.write(word, ZERO);
			}
		}
	}
  
}