package edu.temple.cis5590.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Utils {

	private final static String[] COUNTRIES = 
			new String[] { "Denmark", "Finland", "France", "Ireland", "Netherlands", 
					"Norway", "Sweden", "Switzerland", "United Kingdom", "United States" };

	/**
	 * 
	 * @param dirName
	 * @param retainOrigDir
	 */
	public static void resetDirectory(String dirName, boolean retainOrigDir) {
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
	 * @param code
	 * @param outputPath
	 */
	public static void printResultsToTerminal(int code, String outputPath) {
		System.out.println("\n===========================================================================");
		System.out.println("\tJob " + ((code == 0) ? "complete " : "failed")
				+ "! Check log for detailed execution output.");
		System.out.println("===========================================================================");
		//System.out.println("\nFinal Partition Outputs Go Here\n");

		File outputFolder = new File(outputPath);
		if (outputFolder.exists() && outputFolder.isDirectory()) {
			String[] files = outputFolder.list();
			for (String filename: files) {
				// make sure we're only grabbing the partition and rank comparison output files
				if (filename.contains("part") || filename.contains("rank")) {
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
	
}