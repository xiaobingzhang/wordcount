package maxnum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxNumReducer extends
		Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		long tmp = Integer.MIN_VALUE;
		for(LongWritable value : values)
		{
			if(value.get() > tmp){
				tmp = value.get();
			}
		}
		context.write(new Text(), new LongWritable(tmp));
		
	}
}
