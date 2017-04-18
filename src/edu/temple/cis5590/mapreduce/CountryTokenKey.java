package edu.temple.cis5590.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CountryTokenKey implements Writable, WritableComparable<CountryTokenKey>  {
	
	private Text country;	// natural key
	private Text token;
	private int count;		// secondary key
	
	public CountryTokenKey() {}
	
	public void setProps (String country, String token, int count) {
		this.country = new Text(country);
		this.token = new Text(token);
		this.count = count;
	}
	
	public void setProps (Text country, Text token, int count) {
		this.country = country;
		this.token = token;
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
			comparison = (this.count - ctk.getCount());
		}
		return -(comparison);	// sort in DESCENDING order
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}