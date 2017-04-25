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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class CountryTokenRank {
	
	private int tokenCount = 0;
	private String[] rankedTokens;
	
	private String countryName;
	private List<String> matchingCountries = new ArrayList<String>();
	
	public CountryTokenRank(int limit) {
		rankedTokens = new String[limit];
		for (int i = 0; i < limit; i++) {
			rankedTokens[i] = "";
		}
	}
	
	public void parseToken(String line) {
		String[] countryTokenCount = line.split("-");
		countryName = countryTokenCount[0].trim();
		
		String[] tokenCountString = countryTokenCount[1].trim().split("\t");
		rankedTokens[tokenCount] = tokenCountString[0].trim();
		tokenCount++;
	}
	
	public void addMatchingCountry(String matchingCountryName) {
		if (!matchingCountries.contains(matchingCountryName))
			matchingCountries.add(matchingCountryName);
	}
	
	public String getRankedToken(int index) {
		return (rankedTokens[index]);
	}
	
	public String getCountryName() {
		return countryName;
	}
	
	public List<String> getMatchingCountries() {
		return matchingCountries;
	}
	
	public void compare(CountryTokenRank ctr) {
		boolean match = true;
		for (int i = 0; i < rankedTokens.length; i++) {
			if (!rankedTokens[i].equals(ctr.getRankedToken(i))) {
				match = false;
				break;
			}
		}
		
		if (match) {
			addMatchingCountry(ctr.getCountryName());
			ctr.addMatchingCountry(this.countryName);
		}
	}
	
	public void writeMatchesToFile(Configuration conf, Path outputFile) {
		String output = "\n" + countryName.toUpperCase() + " RANK MATCHES:\n\t";
		if (matchingCountries.size() == 0) output += "NONE";
		else {
			for (int i = 0; i < matchingCountries.size(); i++) {
				output += (matchingCountries.get(i));
				if (i < (matchingCountries.size() - 1)) {
					output += ", ";
				}
			}
		}
		
        Utils.writeToFile(conf, outputFile, output);
	}

	/**
	 * 
	 * @param conf
	 * @param outputPath
	 * @param rankLimit
	 */
	public static List<String> compareResults(Configuration conf, String outputPath, int rankLimit) {
		List<CountryTokenRank> ctrList = new ArrayList<CountryTokenRank>();
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
						if (isPartitionFile) {
				            try {
				                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
								CountryTokenRank ctr = new CountryTokenRank(rankLimit);
								boolean contentWritten = false;
								String prevCountry = "";
				                String line;
				                while ((line = br.readLine()) != null) {
				                	// filter out unicode fluff
				                	if (!line.startsWith("crc")) {
				                		String[] countryTokenCount = line.split("-");
				                		String currentCountry = countryTokenCount[0].trim();
				                		
				                		if (!currentCountry.equals(prevCountry)) {
				    		                if (contentWritten) ctrList.add(ctr);
				                			ctr = new CountryTokenRank(rankLimit);
				                			Logger.info("New country to rank: " + currentCountry);
				                			prevCountry = currentCountry;
				                		}
				                		
					                    ctr.parseToken(line);
					                    contentWritten = true;
				                	}
				                }
				                if (contentWritten) ctrList.add(ctr);
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
		
		List<String> out = new ArrayList<>();
		for (int i = 0; i < ctrList.size(); i++) {
			CountryTokenRank ctr = ctrList.get(i);
			for (int j = 0; j < ctrList.size(); j++) {
				if (i != j) ctr.compare(ctrList.get(j)); 
			}
			
			String output = ctr.getCountryName().toUpperCase() + " RANK MATCHES:\n\t";
			List<String> ctrMatches = ctr.getMatchingCountries();
			if (ctrMatches.size() == 0) output += "NONE";
			else {
				for (int j = 0; j < ctrMatches.size(); j++) {
					output += (ctrMatches.get(j));
					if (j < (ctrMatches.size() - 1)) {
						output += ", ";
					}
				}
			}
			out.add(output);
		}
		
		return out;
	}

}