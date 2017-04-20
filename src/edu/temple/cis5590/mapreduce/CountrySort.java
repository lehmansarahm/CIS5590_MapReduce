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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 */
public class CountrySort {

	// ============================================================================================
	//										MAP
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class CountryTokenMapper extends Mapper<Object, Text, CountryTokenKey, IntWritable> {
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
			String[] lineValues = value.toString().split("\t");
			String[] keyValues = lineValues[0].split("-");
			CountryTokenKey ctk = new CountryTokenKey();
			
			String countryName = keyValues[0];
			String token = (keyValues.length >= 2) ? keyValues[1] : keyValues[0];
			int count = (lineValues.length >= 2) ? Integer.parseInt(lineValues[1]) : 0;
			ctk.setProps(countryName, token, count);
			
			Logger.info("Map phase,Parsing intermediate values for:" + ctk.toLongString());
			context.write(ctk, ctk.getCount());
		}
	}

	// ============================================================================================
	//										PARTITION
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class CountryPartitioner extends Partitioner<CountryTokenKey, IntWritable> {
		/**
		 * 
		 * @param key
		 * @param value
		 * @param numReduceTasks
		 */
		@Override
		public int getPartition(CountryTokenKey key, IntWritable value, int numReduceTasks) {
			if (numReduceTasks == 0) {
				Logger.error("No reduce tasks.  All results assigned to partition 0");
				return 0;
			} else return CountryManager.getPartitionForMapKey(key);
		}
	}

	// ============================================================================================
	//										SORT
	// ============================================================================================
	//				ERROR - CTK "COUNT" PROPERTY NOT COMING THROUGH TO NEW PHASE
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class CountryTokenSortComparator extends WritableComparator {
		/**
		 * 
		 */
	    public CountryTokenSortComparator() {
	        super(CountryTokenKey.class, true);
	    }

		/**
		 * 
		 * @param w1
		 * @param w2
		 */
	    @SuppressWarnings("rawtypes")
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	CountryTokenKey key1 = (CountryTokenKey) w1;
	    	CountryTokenKey key2 = (CountryTokenKey) w2;
	    	Logger.info("Sort phase,Sorting values: " + key1.toLongString() + " and " + key2.toLongString());
	    	return -1 * (key1.sortBy(key2));// sort first by country, then by word count (DESCENDING)
	    }
	}

	// ============================================================================================
	//										REDUCE
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class CountryCountReducer extends Reducer<CountryTokenKey, IntWritable, CountryTokenKey, IntWritable> {
		private IntWritable result = new IntWritable();
		private Map<String,Integer> countryCounts = new HashMap<String,Integer>();
		
		public static boolean limitResults = false;
		
		/**
		 * 
		 * @param key
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void reduce(CountryTokenKey key, Iterable<IntWritable> values, Context context) 
						  throws IOException, InterruptedException {

			if (limitResults) {
				String countryName = key.getCountry().toString();
				if (!countryCounts.containsKey(countryName)) {
					// initialize count for country name
					countryCounts.put(countryName, 0);
				}

				int countryCount = countryCounts.get(countryName);
				if (countryCount < 4) sumValues(key, values, context);

				countryCount++;
				countryCounts.put(countryName, countryCount);
			}
			else sumValues(key, values, context);
		}
		
		/**
		 * 
		 * @param key
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		private void sumValues(CountryTokenKey key, Iterable<IntWritable> values, Context context) 
						  throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) sum += val.get(); 
			result.set(sum);
			
			Logger.debug("Reduce phase,Total values found,Key: " + key + ",Count: " + sum);
			context.write(key, result);
		}
	}
  
}