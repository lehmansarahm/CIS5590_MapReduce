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

import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduce extends Configured implements Tool {

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
		int code = ToolRunner.run(new Configuration(), new MapReduce(), args);
		System.exit(code);
	}
	
	/**
	 * 
	 */
	public int run(String[] args) throws Exception {
		String processingType = (args.length >= 1) ? args[0] : DEFAULT_PROCESSING_TYPE;
		String inputPath = (args.length >= 2) ? args[1] : DEFAULT_INPUT_PATH;
		String outputPath = (args.length >= 3) ? args[2] : DEFAULT_OUTPUT_PATH;
		String logsPath = (args.length >= 4) ? args[3] : Logger.DEFAULT_LOGS_PATH;
		
		// instantiate the job
		Date startTime = new Date();
		Configuration conf = this.getConf();
		conf.addResource(new Path("$HADOOP_CONF/core-site.xml"));
		conf.addResource(new Path("$HADOOP_CONF/hdfs-site.xml"));
		Job wordCountJob = Job.getInstance(conf, "wordCount");
		System.setProperty("HADOOP_USER_NAME", "ubuntu");
		
		// if intermediate, output, and log folders already exist, delete 
		// them and all of their contents so we can do a fresh write
		Utils.resetDirectory(conf, outputPath, false);
		Utils.resetDirectory(conf, logsPath, true);
		
		// set common processing properties
		wordCountJob.setJarByClass(WordCountMapper.class);
		FileInputFormat.addInputPath(wordCountJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(wordCountJob, new Path(outputPath));
		
		// set the processing-type-specific properties
		switch (processingType.toLowerCase()) {
			case "popular":
			    wordCountJob.setMapperClass(WordCountMapper.PopularWordCountMapper.class);
				wordCountJob.setReducerClass(WordCountReducer.PopularWordCountReducer.class);
				break;
			default:
			    wordCountJob.setMapperClass(WordCountMapper.TargetWordCountMapper.class);
				wordCountJob.setReducerClass(WordCountReducer.TargetWordCountReducer.class);
				break;
		}
		
		// set common output properties
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(Text.class);
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(IntWritable.class);
		
		// run the job!
		int completionCode = wordCountJob.waitForCompletion(true) ? 0 : 1;
		Date finishTime = new Date();
		
		// print results and finish up
		// Logger.writeResultsToLog(conf);
		Utils.printResultsToTerminal(completionCode, startTime, finishTime, conf, outputPath);
		if (!processingType.toLowerCase().equals("popular")) {
			List<String> matches = CountryTokenRank.compareResults(conf, outputPath, Utils.TARGET_WORDS.length);
			for (String match : matches) System.out.println(match);
			System.out.println("===========================================================================");
		}
		return completionCode;
	}
	
}