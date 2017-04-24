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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {
	
	public final static String[] TARGET_WORDS = 
			new String[] { "economy", "education", "government", "sports" };
	
	public static enum WORD_COUNT_MODE { Target, Popular };
	private static WORD_COUNT_MODE mode = WORD_COUNT_MODE.Target;

	// ============================================================================================
	//										MAP
	// ============================================================================================

	/**
	 * 
	 */
	public static class WordCountMapper extends Mapper<Object, Text, Text, Text> {

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
			boolean isTargetMode = (mode == WORD_COUNT_MODE.Target);
			boolean isPopularMode = (mode == WORD_COUNT_MODE.Popular);
			String countryName = Utils.getCountryName(context);
			countryText.set(countryName);
			
			Map<String,Integer> tokens = new HashMap<String,Integer>();
			if (isTargetMode) {
				for (int i = 0; i < TARGET_WORDS.length; i++) {
					tokens = insert(tokens, TARGET_WORDS[i], 0);
				}
			}
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken().replaceAll("[^A-Za-z0-9 ]", "").trim().toLowerCase();
				if (isPopularMode && token.length() >= 5) {
					tokens = insert(tokens, token, 1);
				} else if (isTargetMode) {
					for (int i = 0; i < TARGET_WORDS.length; i++) {
						if (token.contains(TARGET_WORDS[i])) {
							tokens = insert(tokens, TARGET_WORDS[i], 1);
							break;
						}
					}
				}
			}
			
			// only write the most popular tokens to the context
			int writeCount = 0;
			int writeLimit = (isPopularMode ? 3 : TARGET_WORDS.length);
			List<Map.Entry<String, Integer>> rankedTokens = rankTokenMap(tokens);
			for (Map.Entry<String, Integer> pair : rankedTokens) {
				if (writeCount < writeLimit) {
			        tokenText.set((String)pair.getKey() + "-" + (Integer)pair.getValue());
			        context.write(countryText, tokenText);
			        writeCount++;
				} else break;
			}
		}
		
	}
	
	/**
	 * 
	 */
	public static class WordCountPartitioner extends Partitioner<Text, Text> {
		
		/**
		 * 
		 */
		@Override
		public int getPartition(Text key, Text val, int numPartitions) {
		    int partition = 0;
		    for (int i = 0; i < Utils.COUNTRIES.length; i++) {
		    	if (key.equals(Utils.COUNTRIES[i])) {
		    		partition = i;
		    		break;
		    	}
		    }
		    return partition;
		}
	 
	}

	// ============================================================================================
	//											REDUCE
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class WordCountReducer extends Reducer<Text, Text, Text, IntWritable> {
		
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
			Map<String,Integer> reducedTokens = new HashMap<String,Integer>();
			for (Text val : values) {
				String[] tokenCount = val.toString().split("-");
				reducedTokens = insert(reducedTokens, tokenCount[0], Integer.parseInt(tokenCount[1]));
			}
			
			// only write the most popular tokens to the context
			int writeCount = 0;
			int writeLimit = (mode == WORD_COUNT_MODE.Popular ? 3 : TARGET_WORDS.length);
			List<Map.Entry<String, Integer>> rankedTokens = rankTokenMap(reducedTokens);
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

	// ============================================================================================
	//											COMMON
	// ============================================================================================

	/**
	 * 
	 * @param newMode
	 */
	public static void setMode(WORD_COUNT_MODE newMode) {
		mode = newMode;
	}
	
	/**
	 * 
	 * @param map
	 * @param key
	 * @param val
	 * @return
	 */
	public static Map<String,Integer> insert(Map<String,Integer> map, String key, int val) {
		if (!map.containsKey(key)) map.put(key,val);
		if (val > 0) {
			int oldVal = map.get(key);
			map.put(key, (val + oldVal));
		}
		return map;
	}
	
	/**
	 * 
	 * @param tokenMap
	 * @return
	 */
	public static List<Map.Entry<String, Integer>> rankTokenMap(Map<String,Integer> tokenMap) {
		List<Map.Entry<String, Integer>> list =
		        new LinkedList<Map.Entry<String, Integer>>(tokenMap.entrySet());
		
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
		    public int compare(Map.Entry<String, Integer> o1,
		                       Map.Entry<String, Integer> o2) {
		        return -1 * (o1.getValue()).compareTo(o2.getValue());
		    }
		});
		
		return list;
	}
	
}