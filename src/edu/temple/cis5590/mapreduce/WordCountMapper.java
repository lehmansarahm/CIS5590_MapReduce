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
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordCountMapper {

	// ============================================================================================
	//										TARGET
	// ============================================================================================

	/**
	 * 
	 */
	public static class TargetWordCountMapper extends Mapper<Object, Text, Text, Text> {

		private Text countryText = new Text();
		private Text tokenText = new Text();
		
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
			String countryName = Utils.getCountryName(context);
			countryText.set(countryName);
			
			Map<String,Integer> tokens = new HashMap<String,Integer>();
			for (int i = 0; i < Utils.TARGET_WORDS.length; i++) {
				tokens = Utils.insert(tokens, Utils.TARGET_WORDS[i], 0);
			}
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken().replaceAll("[^A-Za-z0-9 ]", "").trim().toLowerCase();
				for (int i = 0; i < Utils.TARGET_WORDS.length; i++) {
					if (token.contains(Utils.TARGET_WORDS[i])) {
						tokens = Utils.insert(tokens, Utils.TARGET_WORDS[i], 1);
						break;
					}
				}
			}
			
			// only write the most popular tokens to the context
			writePopularTokens(Utils.WORD_COUNT_MODE.Target, tokens, countryText, tokenText, context);
		}
		
	}

	// ============================================================================================
	//										POPULAR
	// ============================================================================================

	/**
	 * 
	 */
	public static class PopularWordCountMapper extends Mapper<Object, Text, Text, Text> {

		private Text countryText = new Text();
		private Text tokenText = new Text();
		
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
			String countryName = Utils.getCountryName(context);
			countryText.set(countryName);
			
			Map<String,Integer> tokens = new HashMap<String,Integer>();
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken().replaceAll("[^A-Za-z0-9 ]", "").trim().toLowerCase();
				if (token.length() >= 5) tokens = Utils.insert(tokens, token, 1);
			}

			// only write the most popular tokens to the context
			writePopularTokens(Utils.WORD_COUNT_MODE.Popular, tokens, countryText, tokenText, context);
		}
		
	}

	// ============================================================================================
	//											COMMON
	// ============================================================================================
	
	/**
	 * 
	 * @param mode
	 * @param tokens
	 * @param countryText
	 * @param tokenText
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void writePopularTokens(Utils.WORD_COUNT_MODE mode, Map<String,Integer> tokens, 
										  Text countryText, Text tokenText, Context context)
										  throws IOException, InterruptedException {
		int writeCount = 0;
		int writeLimit = (mode == Utils.WORD_COUNT_MODE.Popular ? 3 : Utils.TARGET_WORDS.length);
		List<Map.Entry<String, Integer>> rankedTokens = Utils.rankTokenMap(tokens);
		for (Map.Entry<String, Integer> pair : rankedTokens) {
			if (writeCount < writeLimit) {
		        tokenText.set((String)pair.getKey() + "-" + (Integer)pair.getValue());
		        context.write(countryText, tokenText);
		        writeCount++;
			} else break;
		}
	}
	
}