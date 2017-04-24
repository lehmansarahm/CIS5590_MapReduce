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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Utils {

	public final static String[] COUNTRIES = 
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
	 * @param inputDirName
	 * @param outputDirName
	 * @throws FileNotFoundException 
	 */
	public static void parseInputFiles(String inputDirName, String outputDirName) throws IOException {
		File outFolder = new File(outputDirName);
		if (!outFolder.exists()) outFolder.mkdir();
		
		int counter = 0;
		File inFolder = new File(inputDirName);
		if (inFolder.exists() && inFolder.isDirectory()) {
			String[] files = inFolder.list();
			for (String file: files) {
				// retrieve original input file
				File currentFile = new File(inFolder.getPath(), file);
				String fileName = currentFile.getName();
				String country = fileName.substring(0, fileName.indexOf("."));
				
				// parse original input file
				List<String> fileChunks = readFile(currentFile);
				for (String fileChunk : fileChunks) {
					// write new intermediate file
		        	String newFileName = country + "." + counter + ".txt";
		            File newFile = new File(outFolder.getPath(), newFileName);
		            writeToFile(newFile, fileChunk);
		            counter++;
				}
			}
		} else Logger.error("CANNOT PARSE INPUT FILES FROM DIRECTORY: " + inputDirName);
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
				boolean isPartitionFile = filename.contains("part");
				boolean isRankMatchFile = filename.contains("rank");
				if (isPartitionFile || isRankMatchFile) {
					File currentFile = new File(outputFolder.getPath(), filename);
		            try {
		                BufferedReader br = new BufferedReader(new FileReader(currentFile));
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
	 * @param file
	 * @return
	 */
	public static List<String> readFile(File file) {
		List<String> out = new ArrayList<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
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
	 * @param entry
	 */
	public static void writeToFile(File file, String fileContents) {
        try {
            if (!file.exists()) file.createNewFile();
            FileOutputStream fos = new FileOutputStream(file);
            OutputStreamWriter osw = new OutputStreamWriter(fos);
            osw.append(fileContents);

            osw.close();
            fos.flush();
            fos.close();
        } catch (IOException e) {
            System.out.println("File write failed: " + e.toString());
        }
	}
	
}