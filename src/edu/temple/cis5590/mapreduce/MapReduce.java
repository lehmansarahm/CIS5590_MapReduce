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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.temple.cis5590.mapreduce.WordCount.WORD_COUNT_MODE;

public class MapReduce {

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
		String processingType = (args.length >= 1) ? args[0] : DEFAULT_PROCESSING_TYPE;
		String inputPath = (args.length >= 2) ? args[1] : DEFAULT_INPUT_PATH;
		String outputPath = (args.length >= 3) ? args[2] : DEFAULT_OUTPUT_PATH;
		String logsPath = (args.length >= 4) ? args[3] : Logger.DEFAULT_LOGS_PATH;
		
		// if intermediate, output, and log folders already exist, delete 
		// them and all of their contents so we can do a fresh write
		Utils.resetDirectory(outputPath, false);
		Utils.resetDirectory(logsPath, true);
		
		// instantiate the job
		Configuration conf = new Configuration();
		Job wordCountJob = Job.getInstance(conf, "wordCount");
		FileInputFormat.addInputPath(wordCountJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(wordCountJob, new Path(outputPath));
		
		// set common partition properties
		wordCountJob.setPartitionerClass(WordCount.WordCountPartitioner.class);
		wordCountJob.setNumReduceTasks(Utils.COUNTRIES.length);
		
		// set common processing properties
		wordCountJob.setJarByClass(WordCount.class);
	    wordCountJob.setMapperClass(WordCount.WordCountMapper.class);
		wordCountJob.setReducerClass(WordCount.WordCountReducer.class);
		
		// set the processing-type-specific properties
		switch (processingType.toLowerCase()) {
			case "popular":
			    WordCount.setMode(WORD_COUNT_MODE.Popular);
				break;
			default:
			    WordCount.setMode(WORD_COUNT_MODE.Target);
				break;
		}
		
		// set common output properties
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(Text.class);
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(IntWritable.class);
		
		// run the job!
		int completionCode = wordCountJob.waitForCompletion(true) ? 0 : 1;
		
		// print results and finish up
		Logger.writeResultsToLog();
		Utils.printResultsToTerminal(completionCode, outputPath);
		System.exit(completionCode);
	}

}