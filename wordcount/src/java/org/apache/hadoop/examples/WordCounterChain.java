package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCounterChain extends Configured implements Tool {
	public static class Tokenizer extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class UpperCaser extends MapReduceBase implements
			Mapper<Text, IntWritable, Text, IntWritable> {
		public void map(Text key, IntWritable count,
				OutputCollector<Text, IntWritable> collector, Reporter reporter)
				throws IOException {
			collector.collect(new Text(key.toString().toUpperCase()), count);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> collector, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			result.set(sum);
			collector.collect(key, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		JobConf jobConf = new JobConf(getConf(), WordCounterChain.class);

		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		//删除输出的文件夹
		//设置多个map任务一个reduce任务
		Path outputDir = new Path(args[1]);
		outputDir.getFileSystem(getConf()).delete(outputDir, true);

		JobConf tokenizerMapConf = new JobConf(false);
		ChainMapper.addMapper(jobConf, Tokenizer.class, LongWritable.class,
				Text.class, Text.class, IntWritable.class, true,
				tokenizerMapConf);
		
		JobConf upperCaserMapConf = new JobConf(false);
		ChainMapper.addMapper(jobConf, UpperCaser.class, Text.class,
				IntWritable.class, Text.class, IntWritable.class, true,
				upperCaserMapConf);

		JobConf reduceConf = new JobConf(false);
		ChainReducer.setReducer(jobConf, Reduce.class, Text.class,
				IntWritable.class, Text.class, IntWritable.class, true,
				reduceConf);

		JobClient.runJob(jobConf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new WordCounterChain(),
				args);
		System.exit(ret);

	}
}
