package mytestkv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Text, Text, Text, IntWritable> {
	@Override
	protected void map(Text key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String record = key.toString();
		String course = value.toString();

		System.out.println("key-----"+key+"value-----"+value);
		
		context.write(new Text(course), new IntWritable(Integer.parseInt(record)) );
	}
}
