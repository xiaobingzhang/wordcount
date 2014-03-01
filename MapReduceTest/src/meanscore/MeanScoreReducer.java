package meanscore;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MeanScoreReducer extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values,
			Context context) throws IOException, InterruptedException {
		float sum = 0;
		float count = 0;
		for (FloatWritable value : values) {
			sum += value.get();
			count++;
		}
		FloatWritable meanScore = new FloatWritable(sum / count);
		context.write(key, meanScore);
	}
}
