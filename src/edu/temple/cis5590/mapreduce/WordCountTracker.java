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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 */
public class WordCountTracker implements WritableComparable<WordCountTracker> {

	private Text countryName = new Text();
	private IntWritable totalWordCount = new IntWritable(0);
	private IntWritable limit = new IntWritable();
	
	private Map<String,Integer> words = 
			new HashMap<String,Integer>();
	
	/**
	 * 
	 */
	public WordCountTracker() {}
	
	/**
	 * 
	 * @param limit
	 */
	public WordCountTracker(int limit) {
		this.limit.set(limit);
	}
	
	/**
	 * 
	 * @param limit
	 */
	public WordCountTracker(String country, int limit) {
		this.countryName.set(country);
		this.limit.set(limit);;
	}
	
	/**
	 * 
	 * @param token
	 */
	public void insert(String token) {
		insertToken(token, 1);
		totalWordCount.set(totalWordCount.get() + 1);
	}
	
	/**
	 * 
	 * @param wc
	 */
	@SuppressWarnings("rawtypes")
	public void insert(WordCountTracker wc) {
		Map<String,Integer> rankedTokens = wc.getRankedTokens();
		
		Iterator it = rankedTokens.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        String token = (String)pair.getKey();
	        int count = (Integer)pair.getValue();
	        insertToken(token, count);
	    }

		totalWordCount.set(totalWordCount.get() 
				+ wc.getTotalWordCount());
	}
	
	/**
	 * 
	 * @param token
	 * @param count
	 */
	public void insertToken(String token, int count) {
		if (!words.containsKey(token)) words.put(token, 0); 
		int newCount = (words.get(token) + count);
		words.put(token, newCount);
		
		if (token.equals(null) || token == null) {
			Logger.error("ATTEMPTING TO INSERT NULL TOKEN FOR COUNTRY: " + countryName);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	public int getTotalWordCount() {
		return totalWordCount.get();
	}
	
	/**
	 * 
	 * @return
	 */
	public Text getCountryName() {
		return countryName;
	}
	
	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map<String,Integer> getRankedTokens() {
	    Map<String,Integer> rankedTokenMap = new HashMap<String,Integer>();
	    if (words.size() > 0) {
			String[] rankedTokens = new String[limit.get()];
			int[] ranks = new int[limit.get()];
			
			Iterator it = words.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        String wordToken = (String)pair.getKey();
		        int wordCount = (Integer)pair.getValue();
		        
		        String lastRankedToken = (rankedTokens[limit.get() - 1]);
		        int lastRankedCount = (ranks[limit.get() - 1]);
		        
		        // if the current token has a count higher than 
		        // the values in the current rankings, OR it has 
		        // the same token count as existing entries, but 
		        // has an alphabetically higher key, insert into list
		        if ((wordCount > lastRankedCount) || 
		        		(wordCount == lastRankedCount && 
		        		(lastRankedToken == null || wordToken.compareTo(lastRankedToken) > 0))) {
		        	for (int i = 0; i < limit.get(); i++) {
		        		// move down through the list until we find 
		        		// the next smallest value
		        		if (wordCount > (ranks[i]) || 
		        				(wordCount == ranks[i] && 
		        				(rankedTokens[i] == null || wordToken.compareTo(rankedTokens[i]) > 0))) {
		        			// make space in the rest of the rankings
		        			for (int j = (limit.get() - 2); j >= i; j--) {
		        				rankedTokens[j + 1] = rankedTokens[j];
		        				ranks[j + 1] = ranks[j];
		        			}
		        			
		        			// insert new ranked token
		        			rankedTokens[i] = wordToken;
		        			ranks[i] = wordCount;
		        			break;
		        		}
		        	}
		        }
		        
		        // avoids a ConcurrentModificationException
		        it.remove(); 
		    }
		    
		    // convert arrays to hashmap
		    for (int i = 0; i < limit.get(); i++) {
		    	String rankedToken = rankedTokens[i];
		    	if (rankedToken != null && !rankedToken.equals("")) {
		    		rankedTokenMap.put(rankedToken, ranks[i]);
		    	}
		    }
	    }
	    
		return rankedTokenMap;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("rawtypes")
	public String toString() {
		Map<String,Integer> rankedTokens = getRankedTokens();
		String out = "\n\tTOKEN\t\t\tCOUNT";
		
		Iterator it = rankedTokens.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        String wordToken = (String)pair.getKey();
	        int wordCount = (Integer)pair.getValue();
	        out += ("\n\t" + wordToken + "\t\t" + wordCount);
	    }
	    
	    out += ("\n\tTotal Word Count:\t" + totalWordCount);
		return out;
	}

	/**
	 * 
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		totalWordCount.readFields(in);
		countryName.readFields(in);
		limit.readFields(in);

		boolean hasEntriesToWrite = in.readBoolean();
		if (hasEntriesToWrite) {
			int size = in.readInt();
			Logger.info("Reading in: " + size 
				+ " map entries,For country: " + countryName);

			Text token = new Text();
			IntWritable count = new IntWritable();
			for (int i = 0; i < size; i++) {
				token.readFields(in);
				count.readFields(in);
				words.put(token.toString(), count.get());
			}
		}
	}

	/**
	 * 
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void write(DataOutput out) throws IOException {
		totalWordCount.write(out);
		countryName.write(out);
		limit.write(out);
		
		int mapEntrySize = words.size();
		boolean hasEntriesToWrite = (mapEntrySize != 0);
		out.writeBoolean(hasEntriesToWrite);
		
		if (hasEntriesToWrite) {
			out.writeInt(mapEntrySize);			
			Iterator it = words.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        String entryKey = (String)pair.getKey();
		        int entryValue = (int)pair.getValue();
		        Logger.info("Writing new entry with key: " 
		        		+ entryKey + ",And value: " + entryValue);

				Text token = new Text();
		        token.set(entryKey);
		        token.write(out);

				IntWritable count = new IntWritable();
		        count.set(entryValue);
		        count.write(out);
		    }
		}
	}

	/**
	 * 
	 */
	@Override
	public int compareTo(WordCountTracker wc) {
		return 0; // this.countryName.compareTo(wc.getCountryName());
	}
	
}