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

public class WordCountReducer {

	// ============================================================================================
	//											TARGET
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class TargetWordCountReducer extends Reducer<Text, Text, Text, IntWritable> {
		
		private Text tokenText = new Text();
		private IntWritable totalCount = new IntWritable();
		
		/**
		 * 
		 * @param key
		 * @param values
		 * @param context
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
	
	/**
	 * 
	 */
	public static class PopularWordCountReducer extends Reducer<Text, Text, Text, IntWritable> {
		
		private Text tokenText = new Text();
		private IntWritable totalCount = new IntWritable();
		
		/**
		 * 
		 * @param key
		 * @param values
		 * @param context
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
	 * 
	 * @param values
	 * @return
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
	 * 
	 * @param mode
	 * @param key
	 * @param reducedTokens
	 * @param tokenText
	 * @param totalCount
	 * @param context
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
