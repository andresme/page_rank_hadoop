package com.bda.pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FileMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	String initialPageRank = "1.0";

	@Override
	public void map(LongWritable mapKey, Text mapValue,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		/*
		 * Parse the files and map them to the reducer
		 * Inserts the initial pagerank value (1.0)
		 */
		String stringValue = mapValue.toString();
		String key = "";
		String value = "";

		StringTokenizer tokenizer = new StringTokenizer(stringValue, " ");

		boolean first = true;
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			if (first) {
				first = false;
				key += token;
				value += "1.0\t";
			} else {
				value += token;
				if(tokenizer.hasMoreElements()){
					value += ",";
				}
			}
		}

		output.collect(new Text(key), new Text(value));

	}

}
