package com.bda.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable mapKey, Text mapValue,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		
		/*
		 * This mapper counts the links in the pages
		 * and also saves the links for the reducer and future iterations.
		 */
		
		int pageTabIndex = mapValue.find("\t");
        int rankTabIndex = mapValue.find("\t", pageTabIndex+1);
        
        String page = Text.decode(mapValue.getBytes(), 0, pageTabIndex);
        String pageWithRank = Text.decode(mapValue.getBytes(), 0, rankTabIndex+1);
        
        if(rankTabIndex == -1) return;
        
        String links = Text.decode(mapValue.getBytes(), rankTabIndex+1, mapValue.getLength()-(rankTabIndex+1));

        if(links.equals("")) return;
        
        String[] allOtherPages = links.split(",");

        int totalLinks = allOtherPages.length;
        
        for (String otherPage : allOtherPages){
            Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
            output.collect(new Text(otherPage), pageRankTotalLinks);
        }
        
        output.collect(new Text(page), new Text("|"+links));
        
	}

}
