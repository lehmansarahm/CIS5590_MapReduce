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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 */
public class WordCount {

	private static final String DEFAULT_PROCESSING_TYPE = "base";
	private static final String DEFAULT_INPUT_PATH = "input";
	private static final String DEFAULT_OUTPUT_PATH = "output";

	/**
	 * Expected arguments:
	 * args[0] = word count processing type
	 * args[1] = input path
	 * args[2] = output path (to be overwritten if already exists)
	 */
	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "wordCount");

	    String processingType = (args.length >= 1) ? args[0] : DEFAULT_PROCESSING_TYPE;
	    String inputPath = (args.length >= 2) ? args[1] : DEFAULT_INPUT_PATH;
	    String outputPath = (args.length >= 3) ? args[2] : DEFAULT_OUTPUT_PATH;
	    String logsPath = (args.length >= 4) ? args[3] : Logger.DEFAULT_LOGS_PATH;
	    
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    
	    // if output and log folders already exist, delete them and all of their contents
	    // so we can do a fresh write
	    resetDirectory(outputPath, false);
	    resetDirectory(logsPath, true);
	    
	    // set the processing type specific classes
	    switch (processingType.toLowerCase()) {
	    case "popular":
		    job.setJarByClass(PopularWordCount.class);
		    job.setMapperClass(PopularWordCount.TokenizerMapper.class);
	    	break;
    	default:
    	    job.setJarByClass(BaseWordCount.class);
    	    job.setMapperClass(BaseWordCount.TokenizerMapper.class);
    		break;
	    }
	    
	    // set common properties
	    job.setPartitionerClass(CountryPartitioner.class);
	    job.setNumReduceTasks(CountryManager.COUNTRIES.length);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    // finish the job ... if completes successfully, dump log to file
	    if (job.waitForCompletion(true)) {
	    	Logger.writeResultsToLog();
	    	System.exit(0);
	    } else System.exit(1);
	  }
	  
	/**
	 * 
	 * @param dirName
	 */
	private static void resetDirectory(String dirName, boolean retainOrigDir) {
		File folder = new File(dirName);
		if (folder.exists() && folder.isDirectory()) {
			String[] files = folder.list();
			for (String file: files) {
				File currentFile = new File(folder.getPath(), file);
				currentFile.delete();
			}
			if (!retainOrigDir) folder.delete();
		} else if (retainOrigDir) folder.mkdir();
	}

	// ============================================================================================
	//										PARTITION
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class CountryPartitioner extends Partitioner<Text, IntWritable> {
		/**
		 * 
		 * @param key
		 * @param value
		 * @param numReduceTasks
		 */
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			if (numReduceTasks == 0) {
				Logger.error("No reduce tasks.  All results assigned to partition 0");
				return 0;
			} else return CountryManager.getPartitionForMapKey(key);
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
			context.write(CountryManager.getTextForMapKey(key), result);
		}
	}
	  
}