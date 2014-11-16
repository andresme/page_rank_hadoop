package com.bda.pagerank;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRankDriver extends Configured implements Tool {

	private static NumberFormat nf = new DecimalFormat("00");

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new PageRankDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		// Job 1: Parse files
		parseFiles("pagerank/in/file", "pagerank/ranking/iter00");

		int runs = 0;
		for (; runs < 10; runs++) {
			// Job 2: Calculate new rank
			runPagerank("pagerank/ranking/iter" + nf.format(runs),
					"pagerank/ranking/iter" + nf.format(runs + 1));
			// Job 3: Normalize and order rank
			normalize("pagerank/ranking/iter" + nf.format(runs + 1),
					"pagerank/ranking/iter_norm" + nf.format(runs + 1));
		}

		order("pagerank/ranking/iter_norm" + nf.format(10),
				"pagerank/ranking/output");

		return 0;
	}

	private void parseFiles(String inputPath, String outputPath)
			throws IOException {

		JobConf job = new JobConf();
		job.setJarByClass(PageRankDriver.class);
		job.setJobName("Parsing Job");

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(FileMapper.class);
		job.setReducerClass(FileReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		JobClient.runJob(job);
	}

	private void runPagerank(String inputPath, String outputPath)
			throws IOException {
		JobConf job = new JobConf();
		job.setJarByClass(PageRankDriver.class);
		job.setJobName("Ranking Job");

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		JobClient.runJob(job);
	}

	private void normalize(String inputPath, String outputPath)
			throws IOException {
		JobConf job = new JobConf();
		job.setJarByClass(PageRankDriver.class);
		job.setJobName("Normalize Job");

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(PageRankNormMapper.class);
		job.setReducerClass(PageRankNormReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		JobClient.runJob(job);
	}

	private void order(String inputPath, String outputPath) throws IOException {
		JobConf job = new JobConf();
		job.setJarByClass(PageRankDriver.class);
		job.setJobName("Order Job");

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(PageRankOrderMapper.class);

		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);

		JobClient.runJob(job);
	}

}
