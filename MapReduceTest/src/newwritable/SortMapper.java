package newwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text, Text, StringPair, IntWritable> {
	public StringPair strPair = new StringPair();

	protected void map(Text key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		int intValue = Integer.parseInt(value.toString());
		System.out.println("key-----"+key+"---value-----"+value);
		strPair.setFirstKey(key.toString());
		strPair.setSecondKey(intValue);
		context.write(strPair, new IntWritable(intValue));
	};
}
