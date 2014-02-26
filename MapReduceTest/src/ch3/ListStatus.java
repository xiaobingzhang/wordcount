package ch3;

// cc ListStatus Shows the file statuses for a collection of paths in a Hadoop filesystem 
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

// vv ListStatus
public class ListStatus {

	public static void main(String[] args) throws Exception {
		List<String> argList = new ArrayList<String>();
		argList.add("hdfs://192.168.7.128:9000/user");
		argList.add("hdfs://192.168.7.128:9000/user/root");
		String uri = argList.get(0);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		Path[] paths = new Path[2];
		for (int i = 0; i < paths.length; i++) {
			paths[i] = new Path(argList.get(i));
		}
		
		FileStatus[] status = fs.listStatus(paths);
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p : listedPaths) {
			System.out.println(p);
		}
	}
}
// ^^ ListStatus
