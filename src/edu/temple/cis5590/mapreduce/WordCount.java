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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Text tokenText = new Text();
		private IntWritable countIW = new IntWritable();
		
		/**
		 * 
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@SuppressWarnings("rawtypes")
		public void map(Object key, Text value, Context context) 
					    throws IOException, InterruptedException {
			boolean isTargetMode = (mode == WORD_COUNT_MODE.Target);
			boolean isPopularMode = (mode == WORD_COUNT_MODE.Popular);
			
			Map<String,Integer> tokens = new HashMap<String,Integer>();
			if (isTargetMode) {
				for (int i = 0; i < TARGET_WORDS.length; i++) {
					tokens = insert(tokens, TARGET_WORDS[i], 0);
				}
			}
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken().toLowerCase();
				if (isPopularMode) {
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
			String countryName = Utils.getCountryName(context);
			Map<String,Integer> rankedTokenMap = rankTokenMap(tokens);
			Iterator it = rankedTokenMap.entrySet().iterator();
			while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        tokenText.set(countryName + "-" + (String)pair.getKey());
		        countIW.set((Integer)pair.getValue());
		        context.write(tokenText, countIW);
			}
		}
		
	}
	
	/**
	 * 
	 */
	public static class WordCountPartitioner extends Partitioner<Text, IntWritable> {
		
		/**
		 * 
		 */
		@Override
		public int getPartition(Text key, IntWritable val, int numPartitions) {
		    String[] keys = key.toString().split("-");
		    int partition = 0;
		    for (int i = 0; i < Utils.COUNTRIES.length; i++) {
		    	if (keys[0].equals(Utils.COUNTRIES[i])) {
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
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
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
			Logger.info("Reducing word counts for token: " + key);
			int sum = 0;
			for (IntWritable val : values) sum += val.get();
			result.set(sum);
			context.write(key, result);
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
	public static Map<String,Integer> rankTokenMap(Map<String,Integer> tokenMap) {
	    Map<String,Integer> sortedMap = new HashMap<>();
	    List<String> tokens = new ArrayList<>(tokenMap.keySet());
	    Collections.sort(tokens);
	    
	    List<Integer> tokenCounts = new ArrayList<>(tokenMap.values());
	    Collections.sort(tokenCounts);

	    int sortCount = 0;
	    int sortLimit = (mode == WORD_COUNT_MODE.Popular) ? 3 : TARGET_WORDS.length;
	    
	    Iterator<Integer> countIt = tokenCounts.iterator();
	    while (countIt.hasNext()) {
	        int tokenCount = countIt.next();
	        Iterator<String> tokenIt = tokens.iterator();
	        while (tokenIt.hasNext() && sortCount < sortLimit) {
	            String token = tokenIt.next();
	            int comp1 = tokenMap.get(token);
	            int comp2 = tokenCount;
	            if (comp1 == comp2) {
	                tokenIt.remove();
	                sortedMap.put(token, tokenCount);
	                sortCount++;
	                break;
	            }
	        }
	    }
	    
	    return sortedMap;
	}
	
}