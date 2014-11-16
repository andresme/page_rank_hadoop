package com.bda.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankNormMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable mapKey, Text mapValue,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		
		/*
		 * mapper that maps every value to "total" to be able to sum them
		 * on the reducer
		 */
		
		float value = Float.parseFloat(mapValue.toString().split("\t")[1]);

		output.collect(new Text("total"), new Text(value + "\t" + mapValue));

	}

}
