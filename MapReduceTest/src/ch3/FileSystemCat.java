package ch3;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

// cc FileSystemCat Displays files from a Hadoop filesystem on standard output by using the FileSystem directly

// vv FileSystemCat
public class FileSystemCat {

	public static void main(String[] args) throws Exception {
		String uri = "hdfs://192.168.7.128:9000/user/root/input1/sample.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = null;
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}

	}
}
// ^^ FileSystemCat
