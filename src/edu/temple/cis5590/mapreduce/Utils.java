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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Public reference class with utilitarian methods for general use
 */
public class Utils {

	public final static String[] COUNTRIES = 
			new String[] { "Denmark", "Finland", "France", "Ireland", "Netherlands", 
					"Norway", "Sweden", "Switzerland", "United Kingdom", "United States" };

	public final static String[] TARGET_WORDS = 
			new String[] { "economy", "education", "government", "sports" };
	
	public static enum WORD_COUNT_MODE { Target, Popular };
	
	/**
	 * Deletes the provided folder and any contents
	 * @param conf - the configuration to use
	 * @param outputPath - the folder to delete
	 * @param retainOrigDir - if folder should be retained, recreate after delete
	 */
	public static void resetDirectory(Configuration conf, String outputPath, boolean retainOrigDir) {
		try {
			FileSystem fs = FileSystem.get(conf);
			Path outputFolder = new Path(outputPath);
		    if (fs.exists(outputFolder)) fs.delete(outputFolder, true);
		    if (retainOrigDir) fs.mkdirs(outputFolder);
		} catch (IOException ex) { /* do something */ }
	}
	
	/**
	 * Dumps final execution results to terminal
	 * 
	 * @param code - execution result code from mapreduce job
	 * @param startTime - the time the job started
	 * @param finishTime - the time the job finished
	 * @param conf - the configuration to use
	 * @param outputPath - folder of output files to analyze
	 */
	public static void printResultsToTerminal(int code, Date startTime, Date finishTime, Configuration conf, String outputPath) {
		System.out.println("\n===========================================================================");
		System.out.println("\tJob " + ((code == 0) ? "complete " : "failed")
				+ "! Check log for detailed execution output.");
		System.out.println("===========================================================================");
		//System.out.println("\nFinal Partition Outputs Go Here\n");

		try {
			FileSystem fs = FileSystem.get(conf);
			Path outputFolder = new Path(outputPath);
			if (fs.exists(outputFolder) && fs.isDirectory(outputFolder)) {
				RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(outputFolder, true);
				while(fileIterator.hasNext()){
			        LocatedFileStatus fileStatus = fileIterator.next();
			        if (fileStatus.isFile()) {
						// make sure we're only grabbing the partition and rank comparison output files
			        	String filename = fileStatus.getPath().getName();
						boolean isPartitionFile = filename.contains("part");
						boolean isRankMatchFile = filename.contains("rank");
						if (isPartitionFile || isRankMatchFile) {
				            try {
				                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
				                boolean contentWritten = false;
				                
				                String line;
				                String prevCountry = "";
				                while ((line = br.readLine()) != null) {
				                	// filter out unicode fluff
				                	if (!line.startsWith("crc")) {
				                		String[] countryTokenCount = line.split("-");
				                		String currentCountry = countryTokenCount[0].trim();
				                		if (!isRankMatchFile && !currentCountry.equals(prevCountry)) {
						                	System.out.println("===========================================================================");
						                	prevCountry = currentCountry;
				                		}
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
		} catch (IOException ex) { /* do something */ }
		
		long diffMillisec = finishTime.getTime() - startTime.getTime();
        double diffSec = (double)diffMillisec / 1000.0;
		System.out.println("Time to Complete (sec):\t" + diffSec);
		System.out.println("===========================================================================");
	}
	
	/**
	 * Returns the associated country name for the current mapping context
	 * @param context - the context to use
	 * @return the country name for the current input file
	 */
	@SuppressWarnings("rawtypes")
	public static String getCountryName(Context context) {
		String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
		String countryName = filename.substring(0, filename.indexOf("."));	// strip extension
		
		for (int i = 0; i < COUNTRIES.length; i++) {
			String shortCountryName = COUNTRIES[i].replace(" ", "");
			if (countryName.equals(shortCountryName)) {
				countryName = COUNTRIES[i];
				break;
			}
		}
		
		return countryName;
	}

	/**
	 * Inserts a key into a provided map
	 * @param map - the map to insert into
	 * @param key - the key to use
	 * @param val - the value to use
	 * @return the updated map
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
	 * Sorts and ranks the contents of a token map
	 * @param tokenMap - the map to sort and rank
	 * @return the updated map
	 */
	public static List<Map.Entry<String, Integer>> rankTokenMap(Map<String,Integer> tokenMap) {
		List<Map.Entry<String, Integer>> list =
		        new LinkedList<Map.Entry<String, Integer>>(tokenMap.entrySet());
		
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
		    public int compare(Map.Entry<String, Integer> o1,
		                       Map.Entry<String, Integer> o2) {
		        return -1 * (o1.getValue()).compareTo(o2.getValue());
		    }
		});
		
		return list;
	}
	
	/**
	 * Reads the contents of the indicated file and returns a string list
	 * @param conf - the configuration to use
	 * @param fileName - the file to read from
	 * @return a String list of the file contents
	 */
	public static List<String> readFile(Configuration conf, String fileName) {
		List<String> out = new ArrayList<String>();
		try {
			FileSystem fs = FileSystem.get(conf);
			Path file = new Path(fileName);

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)));
			int counter = 0;
	        String line, chunk = "";
	        while((line=br.readLine()) != null){
	            chunk += line;
	            counter++;
	            if (counter % 100 == 0) {
	            	out.add(chunk);
	            	chunk = "";
	            }
	        }
	        br.close();
		} catch (IOException e) {
            System.out.println("File write failed: " + e.toString());
		}
		return out;
	}
	
	/**
	 * Dumps the provided text to a file
	 * @param conf - the configuration to use
	 * @param fileName - the name of file to dump to
	 * @param fileContents - the contents to dump
	 */
	public static void writeToFile(Configuration conf, String fileName, String fileContents) {
		Path file = new Path(fileName);
		writeToFile(conf, file, fileContents);
	}

	/**
	 * Dumps the provided text to a file
	 * @param conf - the configuration to use
	 * @param file - the file to dump to
	 * @param fileContents - the contents to dump
	 */
	public static void writeToFile(Configuration conf, Path file, String fileContents) {
        try {
			FileSystem fs = FileSystem.get(conf);
            if (!fs.exists(file)) fs.create(file);
		    
		    FSDataOutputStream fout = fs.create(file);
		    fout.writeUTF(fileContents);
		    fout.close();
        } catch (IOException e) {
            System.out.println("File write failed: " + e.toString());
        }
	}
	
}