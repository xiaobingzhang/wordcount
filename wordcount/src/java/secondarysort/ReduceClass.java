package secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends
		Reducer<IntPair, IntWritable, Text, IntWritable> {

	private static final Text SEPARATOR = new Text(
			"----------------------------------------");
	private final Text first = new Text();

	@Override
	protected void reduce(IntPair key, Iterable<IntWritable> value,
			Context context) throws IOException, InterruptedException {
		context.write(SEPARATOR, null);
		first.set(Integer.toString(key.getFirst()));
		for (IntWritable v : value) {
			context.write(first, v);
		}
	}
}
