package newwritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<StringPair, IntWritable, Text, Text> {
	@Override
	protected void reduce(StringPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		StringBuffer combineValue = new StringBuffer();
		Iterator<IntWritable> itr = values.iterator();
		// 通过设置group函数可以按照StringPair中的firstKey进行group
		System.out.println("*****");
		while (itr.hasNext()) {
			int value = itr.next().get();
			combineValue.append(value + ",");
			System.out.println(key.getFirstKey()+"---"+key.getSecondKey());
		}
		System.out.println("*****");
		String str = "";
		if (combineValue.length() > 0) {
			str = combineValue.substring(0, combineValue.length() - 1);
		}
		context.write(new Text(key.getFirstKey()), new Text(str));
	}
}
