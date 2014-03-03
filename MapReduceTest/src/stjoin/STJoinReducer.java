package stjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class STJoinReducer extends Reducer<Text, Text, Text, Text> {

	public static int time = 0;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if (time == 0) {// 输出表头
			context.write(new Text("grandChild"), new Text("grandParent"));
			time++;
		}
		List<String> grandChild = new ArrayList<String>();
		List<String> grandParent = new ArrayList<String>();
		// analyze the value to decide which is the grandChild and which is the
		// grandParent
		Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			String line = itr.next().toString();
			int recordLength = line.length();
			if (recordLength < 2) {
				continue;
			}
			if (line.charAt(0) == '1') {
				grandChild.add(line.substring(1));
			} else if (line.charAt(0) == '2') {
				grandParent.add(line.substring(1));
			}
		}
		if (grandChild.size() != 0 && grandParent.size() != 0) {
			for (String c : grandChild) {
				for (String p : grandParent) {
					context.write(new Text(c), new Text(p));
				}
			}
		}

	}

}
