package ch3;

// cc FileSystemDoubleCat Displays files from a Hadoop filesystem on standard output twice, by using seek
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

// vv FileSystemDoubleCat
public class FileSystemDoubleCat {

	public static void main(String[] args) throws Exception {

		String uri = "hdfs://192.168.7.128:9000/user/root/input1/sample.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			in.seek(0); // go back to the start of the file
			System.out.println();
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
// ^^ FileSystemDoubleCat
