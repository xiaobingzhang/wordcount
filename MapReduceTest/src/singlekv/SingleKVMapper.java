package singlekv;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SingleKVMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		context.write(value, new Text(""));
	}
}
