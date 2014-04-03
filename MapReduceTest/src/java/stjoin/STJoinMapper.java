package stjoin;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class STJoinMapper extends Mapper<Object, Text, Text, Text> {
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String childName="" ;
		String parentName="" ;
		String relation="" ;
		
		String line = value.toString();
		
		StringTokenizer st = new StringTokenizer(line);
		
		while (st.hasMoreTokens()) {
			childName = st.nextToken();
			parentName = st.nextToken();
		}
		if(!childName.equals("child")){
			relation = "1";//左右表分区标志
			context.write(new Text(parentName),new Text(relation+childName));//左表
			relation = "2";
			context.write(new Text(childName), new Text(relation+parentName));//右表
			
		}
	}
}
