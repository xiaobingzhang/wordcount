package meanscore;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MeanScoreMapper extends Mapper<Object, Text, Text, FloatWritable> {
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokens = new StringTokenizer(line, "\n");
		while (tokens.hasMoreTokens()) {
			String tmp = tokens.nextToken();
			StringTokenizer sz = new StringTokenizer(tmp);
			String name = sz.nextToken();
			float score = Float.valueOf(sz.nextToken());
			context.write(new Text(name), new FloatWritable(score));
		}
	}
}
