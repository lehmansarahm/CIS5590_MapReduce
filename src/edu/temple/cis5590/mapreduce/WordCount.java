// =======================================================================
//
//	Course:		CIS 5590, Spring 2017
//	Professor:	X. He
//	
//	Author:		Sarah M. Lehman
//	Email:		smlehman@temple.edu
//
//	Program:	Semester Project, AWS Hadoop Map-Reduce
//  Source:		https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
//
// =======================================================================

package edu.temple.cis5590.mapreduce;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
	    
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    
	    // if output path already exists, delete it and all of its contents
	    // so we can do a fresh write
	    File outputFolder = new File(outputPath);
	    if (outputFolder.exists() && outputFolder.isDirectory()) {
	    	String[]entries = outputFolder.list();
	    	for(String s: entries){
	    	    File currentFile = new File(outputFolder.getPath(),s);
	    	    currentFile.delete();
	    	}
	    	outputFolder.delete();
	    }
	    
	    // set the necessary classes by processing type
	    switch (processingType.toLowerCase()) {
	    case "popular":
		    job.setJarByClass(PopularWordCount.class);
		    job.setMapperClass(PopularWordCount.TokenizerMapper.class);
		    job.setCombinerClass(PopularWordCount.IntSumReducer.class);
		    job.setReducerClass(PopularWordCount.IntSumReducer.class);
	    	break;
    	default:
    	    job.setJarByClass(BaseWordCount.class);
    	    job.setMapperClass(BaseWordCount.TokenizerMapper.class);
    	    job.setCombinerClass(BaseWordCount.IntSumReducer.class);
    	    job.setReducerClass(BaseWordCount.IntSumReducer.class);
    		break;
	    }
	    
	    // finish the job
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	  
}