package com.bda.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankOrderMapper extends MapReduceBase implements
Mapper<LongWritable, Text, FloatWritable, Text> {

	@Override
	public void map(LongWritable mapKey, Text mapValue,
			OutputCollector<FloatWritable, Text> output, Reporter reporter)
			throws IOException {
		
		/*
		 * This mapper uses a little trick to order (ascending) the page rank results
		 * it uses the value as key and hadoop will order it
		 */
		
		float key = Float.valueOf(mapValue.toString().split("\t")[1]);
		String value = mapValue.toString().split("\t")[0];
		
		output.collect(new FloatWritable(key), new Text(value));
	}

}
