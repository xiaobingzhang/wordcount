package mytest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	enum Status {
		YES, NO
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		
		String line = value.toString();
		String record = line.substring(0, 2);
		String course = line.substring(3);
		System.out.println("key-----" + key + "value-----" + value);
		context.getCounter(Status.YES).increment(1);// 可以设置一个counter然后进行计数，但是这个时候由于设置的enum是在编译的时候确定的不能够动态设置
		context.write(new Text(course),
				new IntWritable(Integer.parseInt(record)));
	}
}
