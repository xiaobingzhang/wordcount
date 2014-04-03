package mytest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private MultipleOutputs<Text, IntWritable> multipleOutputs;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		multipleOutputs =new MultipleOutputs<Text, IntWritable>(context);
		super.setup(context);
	}
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		
		
		multipleOutputs.write(key, new IntWritable(maxValue),String.valueOf(maxValue));
		//context.write(key, new IntWritable(maxValue));
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
		super.cleanup(context);
	}
}
