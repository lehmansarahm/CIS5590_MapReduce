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
	public static class TokenizerMapper extends Mapper<Object, Text, CountryTokenKey, IntWritable> {
		private CountryTokenKey ctk;
		
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
			
			String countryName = keyValues[0];
			String token = (keyValues.length >= 2) ? keyValues[1] : keyValues[0];
			int count = (lineValues.length >= 2) ? Integer.parseInt(lineValues[1]) : 0;
			
			ctk = new CountryTokenKey();
			ctk.setProps(countryName, token, count);
			context.write(ctk, new IntWritable(count));
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
	//										GROUP
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class CountryTokenComparator extends WritableComparator {
		/**
		 * 
		 */
	    public CountryTokenComparator() {
	        super(CountryTokenKey.class);
	    }

		/**
		 * 
		 * @param w1
		 * @param w2
		 */
	    @SuppressWarnings("rawtypes")
		@Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	Logger.info("Comparing values: " + w1.toString() + " and " + w2.toString());
	    	CountryTokenKey key1 = (CountryTokenKey) w1;
	    	CountryTokenKey key2 = (CountryTokenKey) w2;
	    	return (key1.getCountry().compareTo(key2.getCountry()));
	    }
	}

	// ============================================================================================
	//										REDUCE
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class IntSumReducer extends Reducer<CountryTokenKey, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
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
			int sum = 0;
			for (IntWritable val : values) sum += val.get(); 
			result.set(sum);
			context.write(key.getToken(), result);
		}
	}
  
}