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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 */
public class WordCount {

	private static final String DEFAULT_PROCESSING_TYPE = "base";
	private static final String DEFAULT_INPUT_PATH = "input";
	private static final String DEFAULT_INTERMEDIATE_PATH = "intermediate";
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
	    String intermPath = DEFAULT_INTERMEDIATE_PATH;
	    String interm2Path = intermPath + "2";
	    String outputPath = (args.length >= 3) ? args[2] : DEFAULT_OUTPUT_PATH;
	    String logsPath = (args.length >= 4) ? args[3] : Logger.DEFAULT_LOGS_PATH;
	    
	    // if intermediate, output, and log folders already exist, delete 
	    // them and all of their contents so we can do a fresh write
	    resetDirectory(intermPath, false);
	    resetDirectory(interm2Path, false);
	    resetDirectory(outputPath, false);
	    resetDirectory(logsPath, true);
		    
	    Configuration conf = new Configuration();
	    Job wordCountJob = Job.getInstance(conf, "wordCount");
	    FileInputFormat.addInputPath(wordCountJob, new Path(inputPath));
	    FileOutputFormat.setOutputPath(wordCountJob, new Path(intermPath));
	    
	    // set the processing type specific classes
	    boolean limitTopResults = false;
	    switch (processingType.toLowerCase()) {
	    case "popular":
		    wordCountJob.setJarByClass(PopularWordCount.class);
		    wordCountJob.setMapperClass(PopularWordCount.TokenizerMapper.class);
		    wordCountJob.setCombinerClass(IntSumReducer.class);
		    wordCountJob.setReducerClass(IntSumReducer.class);
		    limitTopResults = true;
	    	break;
    	default:
    	    wordCountJob.setJarByClass(BaseWordCount.class);
    	    wordCountJob.setMapperClass(BaseWordCount.TokenizerMapper.class);
    	    wordCountJob.setCombinerClass(IntSumReducer.class);
    	    wordCountJob.setReducerClass(IntSumReducer.class);
    		break;
	    }
	    
	    // set common output properties
	    wordCountJob.setOutputKeyClass(Text.class);
	    wordCountJob.setOutputValueClass(IntWritable.class);

	    // run the job!
	    int code = wordCountJob.waitForCompletion(true) ? 0 : 1;
	    if (code == 0) {
	    	// word count finished successfully ... run the sorting job
		    Job sortJob = Job.getInstance(conf, "wordCountSort");
		    FileInputFormat.addInputPath(sortJob, new Path(intermPath));
		    
		    // depending on whether we are limiting the final 
		    // results, change the output path
		    if (limitTopResults)
		    	FileOutputFormat.setOutputPath(sortJob, new Path(interm2Path));
		    else
		    	FileOutputFormat.setOutputPath(sortJob, new Path(outputPath));

		    // set map-reduce properties
		    sortJob.setJarByClass(CountrySort.class);
		    sortJob.setMapperClass(CountrySort.CountryTokenMapper.class);
		    sortJob.setReducerClass(CountrySort.CountryCountReducer.class);
		    
		    // set processing properties
		    sortJob.setPartitionerClass(CountrySort.CountryPartitioner.class);
		    sortJob.setNumReduceTasks(CountryManager.COUNTRIES.length);
		    sortJob.setGroupingComparatorClass(CountrySort.CountryTokenGroupComparator.class);
		    sortJob.setSortComparatorClass(CountrySort.CountryTokenSortComparator.class);
		    
		    // set output properties
		    sortJob.setMapOutputKeyClass(CountryTokenKey.class);
		    sortJob.setMapOutputValueClass(IntWritable.class);
		    sortJob.setOutputKeyClass(CountryTokenKey.class);
		    sortJob.setOutputValueClass(IntWritable.class);
		    
		    // run the job!
		    code = sortJob.waitForCompletion(true) ? 0 : 1;
		    if (code == 0 && limitTopResults) {
		    	// if evaluating overall popular words, run the result limiter job
			    Job limitJob = Job.getInstance(conf, "wordCountLimit");
			    FileInputFormat.addInputPath(limitJob, new Path(interm2Path));
			    FileOutputFormat.setOutputPath(limitJob, new Path(outputPath));

			    // set map-reduce properties
			    limitJob.setJarByClass(TopResultLimit.class);
			    limitJob.setMapperClass(TopResultLimit.TopResultTokenMapper.class);
			    limitJob.setReducerClass(CountrySort.CountryCountReducer.class);
			    
			    // set processing properties
			    limitJob.setPartitionerClass(CountrySort.CountryPartitioner.class);
			    limitJob.setNumReduceTasks(CountryManager.COUNTRIES.length);
			    limitJob.setGroupingComparatorClass(CountrySort.CountryTokenGroupComparator.class);
			    limitJob.setSortComparatorClass(CountrySort.CountryTokenSortComparator.class);
			    
			    // set output properties
			    limitJob.setMapOutputKeyClass(CountryTokenKey.class);
			    limitJob.setMapOutputValueClass(IntWritable.class);
			    limitJob.setOutputKeyClass(CountryTokenKey.class);
			    limitJob.setOutputValueClass(IntWritable.class);
			    
			    // run the job!
			    code = limitJob.waitForCompletion(true) ? 0 : 1;
		    }
	    }
	    
	    // print results and finish up
    	Logger.writeResultsToLog();
    	printResultsToTerminal(code, outputPath);
    	System.exit(code);
	  }

	// ============================================================================================
	//								PRIVATE REFERENCE METHODS
	// ============================================================================================
		
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
	
	/**
	 * 
	 */
	private static void printResultsToTerminal(int code, String outputPath) {
		System.out.println("\n===========================================================================");
		System.out.println("\tJob " + ((code == 0) ? "complete " : "failed")
				+ "! Check log for detailed execution output.");
		System.out.println("===========================================================================");
		//System.out.println("\nFinal Partition Outputs Go Here\n");

		File outputFolder = new File(outputPath);
		if (outputFolder.exists() && outputFolder.isDirectory()) {
			String[] files = outputFolder.list();
			for (String filename: files) {
				if (filename.contains("part")) {	// make sure we're grabbing the partition output files
					File currentFile = new File(outputFolder.getPath(), filename);
		            try {
		                BufferedReader br = new BufferedReader(new FileReader(currentFile));
		                boolean contentWritten = false;
		                
		                String line;
		                while ((line = br.readLine()) != null) {
		                	// filter out unicode fluff
		                	if (!line.startsWith("crc")) {
			                    System.out.println(line);
			                    contentWritten = true;
		                	}
		                }
		                
		                if (contentWritten) {
		                	System.out.println("===========================================================================");
		                }
		                br.close();
		            } catch (IOException e) {
		                Logger.info("Could not read file: " + e.toString());
		                Logger.error("File read failed: " + e.toString());
		            }
				}
			}
		}
	}

	// ============================================================================================
	//										REDUCER CLASS
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
			context.write(key, result);
		}
	}
	  
}