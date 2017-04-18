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
import org.apache.hadoop.io.IntWritable.Comparator;
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
	    
	    // set common partition properties
	    job.setPartitionerClass(CountryPartitioner.class);
	    job.setNumReduceTasks(CountryManager.COUNTRIES.length);
	    
	    // set common reduce properties
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setSortComparatorClass(ResultSorter.class);
	    
	    // set common output properties
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    // finish the job ... if completes successfully, dump log to file
	    if (job.waitForCompletion(true)) {
	    	Logger.writeResultsToLog();
	    	printResultsToTerminal(outputPath);
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
	
	/**
	 * 
	 */
	private static void printResultsToTerminal(String outputPath) {
		System.out.println("\n===========================================================================");
		System.out.println("\tJob Complete! Check log for detailed execution output.");
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

	// ============================================================================================
	//										SORT
	// ============================================================================================
	
	/**
	 * 
	 */
	public static class ResultSorter extends Comparator {
		
		/**
		 * 
		 * @param iw1
		 * @param iw2
		 * @return
		 */
		public int compare(IntWritable iw1, IntWritable iw2) {
			// return negative integer, zero, or a positive integer if the first argument 
			// is less than, equal to, or greater than the second
			return (iw1.get() - iw2.get());
		}
		
		/**
		 * 
		 * @param c
		 * @return
		 */
		public boolean equals(Comparator c) {
			// don't care about this ... short circuit to "false"
			return false;
		}
		
	}
	  
}