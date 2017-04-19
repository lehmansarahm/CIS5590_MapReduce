package edu.temple.cis5590.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CountryTokenKey implements WritableComparable<CountryTokenKey> { // Writable, WritableComparable<CountryTokenKey>  {
	
	private Text country;	// natural key
	private Text token;
	private int count;		// secondary key
	
	public CountryTokenKey() {
		this.country = new Text();
		this.token = new Text();
		this.count = 0;
	}
	
	public void setProps (String country, String token, int count) {
		this.country.set(country);
		this.token.set(token);
		this.count = count;
	}
	
	public Text getCountry() {
		return this.country;
	}
	
	public Text getToken() {
		return this.token;
	}
	
	public int getCount() {
		return this.count;
	}

	@Override
	public int compareTo(CountryTokenKey ctk) {
		int comparison = this.country.compareTo(ctk.getCountry());
		if (comparison == 0) {
			comparison = this.token.compareTo(ctk.getToken());
		}
		if (comparison == 0) {
			comparison = (this.count - ctk.getCount());
		}
		return comparison;
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
		return this.token.toString();
	}
	
	public String toLongString() {
		return (this.country + "-" + this.token);
	}
	
}