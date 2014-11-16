package com.bda.pagerank;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankNormReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, FloatWritable> {

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, FloatWritable> output, Reporter reporter)
			throws IOException {

		/*
		 * This reducer sums the values of page ranks and also normalize every
		 * value of the page ranks pagerank_value page_id
		 */

		float total = 0.0f;
		HashMap<String, String> map = new HashMap<String, String>();

		while (values.hasNext()) {
			String value = values.next().toString();
			total += Float.valueOf(value.split("\t")[0]);
			String page = value.split("\t")[1];
			String pagerank = value.split("\t")[2];
			map.put(page, pagerank);
		}

		for (Entry<String, String> entry : map.entrySet()) {
			String pagerank = entry.getValue();

			float normalized = Float.valueOf(pagerank) / total;

			output.collect(new Text(entry.getKey()), new FloatWritable(
					normalized));
		}

	}

}
