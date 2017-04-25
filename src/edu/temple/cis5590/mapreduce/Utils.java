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
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Utils {

	public final static String[] COUNTRIES = 
			new String[] { "Denmark", "Finland", "France", "Ireland", "Netherlands", 
					"Norway", "Sweden", "Switzerland", "United Kingdom", "United States" };

	/**
	 * 
	 * @param conf
	 * @param outputPath
	 * @param retainOrigDir
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
	 * 
	 * @param code
	 * @param startTime
	 * @param finishTime
	 * @param conf
	 * @param outputPath
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
	 * 
	 * @param context
	 * @return
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
	 * 
	 * @param fileName
	 * @return
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
	 * 
	 * @param conf
	 * @param fileName
	 * @param fileContents
	 */
	public static void writeToFile(Configuration conf, String fileName, String fileContents) {
		Path file = new Path(fileName);
		writeToFile(conf, file, fileContents);
	}
	
	/**
	 * 
	 * @param conf
	 * @param file
	 * @param fileContents
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