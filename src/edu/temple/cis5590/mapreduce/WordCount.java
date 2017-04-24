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
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {
	
	private final static String[] TARGET_WORDS = 
			new String[] { "economy", "education", "government", "sports" };
	
	public static enum WORD_COUNT_MODE { Target, Popular };
	private static WORD_COUNT_MODE mode = WORD_COUNT_MODE.Target;

	// ============================================================================================
	//										MAP
	// ============================================================================================

	/**
	 * 
	 */
	public static class WordCountMapper extends Mapper<Object, Text, Text, WordCountTracker> {

		private Text countryText = new Text();
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
			boolean isTargetMode = (mode == WORD_COUNT_MODE.Target);
			boolean isPopularMode = (mode == WORD_COUNT_MODE.Popular);
			
			if (isTargetMode && !init) {
				initializeTokens(context);
				init = true;
			}
			
			String countryName = Utils.getCountryName(context);
			countryText.set(countryName);
			WordCountTracker wc = new WordCountTracker(countryName, 
					(isTargetMode ? TARGET_WORDS.length : 3));
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken()/*.replaceAll("[^a-zA-Z ]", "")*/.toLowerCase();
				if (isPopularMode) wc.insert(token);
				else if (isTargetMode) {
					for (int i = 0; i < TARGET_WORDS.length; i++) {
						if (token.contains(TARGET_WORDS[i])) {
							wc.insert(TARGET_WORDS[i]);
							break;
						}
					}
				}
			}
			
			context.write(countryText, wc);
		}
		
		/**
		 * 
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		private void initializeTokens(Context context)
									  throws IOException, InterruptedException {
			String countryName = Utils.getCountryName(context);
			countryText.set(countryName);
			WordCountTracker wc = new WordCountTracker(countryName, TARGET_WORDS.length);
			for (int i = 0; i < TARGET_WORDS.length; i++) {
				wc.insertToken(TARGET_WORDS[i], 0);
			}
			context.write(countryText, wc);
		}
		
	}

	// ============================================================================================
	//											REDUCE
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class WordCountReducer extends Reducer<Text, WordCountTracker, Text, WordCountTracker> {
		/**
		 * 
		 * @param key
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void reduce(Text key, Iterable<WordCountTracker> values, Context context) 
						  throws IOException, InterruptedException {
			WordCountTracker wc = new WordCountTracker(key.toString(), TARGET_WORDS.length);
			Logger.info("Reducing word counts for country: " + key);
			for (WordCountTracker val : values) wc.insert(val);
			context.write(key, wc);
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
	
}