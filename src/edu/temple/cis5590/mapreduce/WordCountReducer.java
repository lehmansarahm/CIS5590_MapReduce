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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * General purpose word count reducer class - provides two reducing classes, 
 * one for "target" words and one for "popular" words
 */
public class WordCountReducer {

	// ============================================================================================
	//											TARGET
	// ============================================================================================
	
	public static class TargetWordCountReducer extends Reducer<Text, Text, Text, IntWritable> {
		
		private Text tokenText = new Text();
		private IntWritable totalCount = new IntWritable();
		
		/**
		 * Reduces the input words according to whether they appear in a list of target words
		 * 
		 * @param key - the name of the country from which the tokens were taken
		 * @param values - the list of "token-count" values for each country
		 * @param context - the reducer context being used
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) 
						   throws IOException, InterruptedException {
			// update the log
			Logger.info("Reducing word counts for country: " + key);
			
			// consolidate the tokens
			Map<String,Integer> reducedTokens = reduceTokens(values);
			
			// only write the most popular tokens to the context
			writePopularTokens(Utils.WORD_COUNT_MODE.Target, key, reducedTokens, 
					tokenText, totalCount, context);
		}
		
	}

	// ============================================================================================
	//											POPULAR
	// ============================================================================================
	
	public static class PopularWordCountReducer extends Reducer<Text, Text, Text, IntWritable> {
		
		private Text tokenText = new Text();
		private IntWritable totalCount = new IntWritable();
		
		/**
		 * Reduces the input words according to whether they are among the three most prevalent 
		 * words in the input file
		 * 
		 * @param key - the name of the country from which the tokens were taken
		 * @param values - the list of "token-count" values for each country
		 * @param context - the reducer context being used
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) 
				  		   throws IOException, InterruptedException {
			// update the log
			Logger.info("Reducing word counts for country: " + key);
			
			// consolidate the tokens
			Map<String,Integer> reducedTokens = reduceTokens(values);
			
			// only write the most popular tokens to the context
			writePopularTokens(Utils.WORD_COUNT_MODE.Popular, key, reducedTokens, 
					tokenText, totalCount, context);
		}
		
	}

	// ============================================================================================
	//											COMMON
	// ============================================================================================
	
	/**
	 * Converts a list of "token-count" values into a map of token-count values
	 * @param values - the "token-count" strings to reduce
	 * @return the consolidated token-count map
	 */
	public static Map<String,Integer> reduceTokens(Iterable<Text> values) {
		Map<String,Integer> reducedTokens = new HashMap<String,Integer>();
		for (Text val : values) {
			String[] tokenCount = val.toString().split("-");
			reducedTokens = Utils.insert(reducedTokens, tokenCount[0], Integer.parseInt(tokenCount[1]));
		}
		return reducedTokens;
	}
	
	/**
	 * Writes provided word to context, to be returned as a final result
	 * 
	 * @param mode - processing mode (either "target" or "popular")
	 * @param key - the name of the country for which tokens are being reduced
	 * @param reducedTokens - the current map of tokens already reduced
	 * @param tokenText - the token word being reduced
	 * @param totalCount - the count of token words being reduced
	 * @param context - the reducing context being used
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void writePopularTokens(Utils.WORD_COUNT_MODE mode, Text key, Map<String,Integer> reducedTokens, 
										  Text tokenText, IntWritable totalCount, Context context)
										  throws IOException, InterruptedException {
		int writeCount = 0;
		int writeLimit = (mode == Utils.WORD_COUNT_MODE.Popular ? 3 : Utils.TARGET_WORDS.length);
		List<Map.Entry<String, Integer>> rankedTokens = Utils.rankTokenMap(reducedTokens);
		for (Map.Entry<String, Integer> pair : rankedTokens) {
			if (writeCount < writeLimit) {
		        tokenText.set(key + " - " + (String)pair.getKey());
		        totalCount.set((Integer)pair.getValue());
		        context.write(tokenText, totalCount);
		        writeCount++;
			} else break;
		}
	}

}
