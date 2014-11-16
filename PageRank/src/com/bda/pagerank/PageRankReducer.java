package com.bda.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private static final float BETA = 0.85f;

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		/*
		 * This reducer Calculates the page rank values
		 */
		
		String[] split;
		float sumShareOtherPageRanks = 0;
		String links = "";
		String pageWithRank;

		while (values.hasNext()) {
			pageWithRank = values.next().toString();

			if (pageWithRank.startsWith("|")) {
				links = "\t" + pageWithRank.substring(1);
				continue;
			}

			split = pageWithRank.split("\t");
			
			float pageRank = Float.valueOf(split[1]);
			int countOutLinks = Integer.valueOf(split[2]);

			sumShareOtherPageRanks += (pageRank / countOutLinks);
		}

		float newRank = BETA * sumShareOtherPageRanks + (1 - BETA);

		output.collect(key, new Text(newRank + links));

	}

}
