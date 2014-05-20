package ch5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxValueMapper extends Mapper<Text, Text, Text, IntWritable> {
	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		
		String t1 = key.toString();
		String t = value.toString();
		System.out.println(t+t1);
		int te = Integer.parseInt(t);
		context.write(new Text("temperature"), new IntWritable(te));
	}
}
