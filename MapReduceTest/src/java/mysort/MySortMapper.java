package mysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class MySortMapper extends Mapper<Text, Text,IntWritable, Text> {
	private Counter counter1,counter2;
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		int val = Integer.parseInt(line);
		if(val > 10){
			counter1 = context.getCounter(ValEnum.MORETHAN100);
			counter1.increment(1);
		}
		else{
			counter2 = context.getCounter(ValEnum.LESSTHAN100);
			counter2.increment(1);
		}
		context.write(new IntWritable(val),key);
	}
}
