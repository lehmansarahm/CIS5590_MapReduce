package edu.temple.cis5590.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CountryTokenKey implements WritableComparable<CountryTokenKey> { // Writable, WritableComparable<CountryTokenKey>  {

	public final static String TOTAL_TOKEN = "total";
	private Text country;		// natural key
	private Text token;
	private IntWritable count;	// secondary key
	
	public CountryTokenKey() {
		this.country = new Text();
		this.token = new Text();
		this.count = new IntWritable();
	}
	
	public void setProps (String country, String token, int count) {
		for (int i = 0; i < CountryManager.COUNTRIES.length; i++) {
			String countryName = CountryManager.COUNTRIES[i].toLowerCase().replace(" ", "");
			if (country.toLowerCase().contains(countryName)) {
				country = CountryManager.COUNTRIES[i];
				break;
			}
		}
		this.country.set(country);
		this.token.set(token);
		this.count.set(count);
	}
	
	public Text getCountry() {
		return this.country;
	}
	
	public Text getToken() {
		return this.token;
	}
	
	public IntWritable getCount() {
		return this.count;
	}

	public int groupBy(CountryTokenKey ctk) {
		// if this key is a country total, it should always come first
		if (token.toString().contains(country.toString())) return -1;
		else {
			// apply the actual comparison
			int comparison = this.country.compareTo(ctk.getCountry());
			if (comparison == 0) {
				comparison = this.token.compareTo(ctk.getToken());
			}
			return comparison;
		}
	}
	
	public int sortBy(CountryTokenKey ctk) {
    	int comparison = country.compareTo(ctk.getCountry());
    	if (comparison == 0) {
    		// if partitioning worked correctly, country 
    		// comparison should always be zero
    		comparison = count.compareTo(ctk.getCount());
    	}
    	return comparison;
	}

	@Override
	public int compareTo(CountryTokenKey ctk) {
		// if this key has the same token and country, it should always come first
		if (country.toString().equals(token.toString())) return -1;
		else {
			// apply the actual comparison
			int comparison = count.compareTo(ctk.getCount());
			if (comparison == 0) {
				comparison = this.token.compareTo(ctk.getToken());
			}
			if (comparison == 0) {
				comparison = this.country.compareTo(ctk.getCountry());
			}
			return comparison;
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.country.readFields(in);
		this.token.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.country.write(out);
		this.token.write(out);
	}
	
	@Override
	public String toString() {
		if (this.token.toString().equals(TOTAL_TOKEN))
			return this.country + " (" + TOTAL_TOKEN + ")";
		else return this.token.toString();
	}
	
	public String toLongString() {
		return (this.country + "-" + this.token + "-" + this.count);
	}
	
}