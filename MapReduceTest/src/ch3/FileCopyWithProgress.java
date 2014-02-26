package ch3;

// cc FileCopyWithProgress Copies a local file to a Hadoop filesystem, and shows progress
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

// vv FileCopyWithProgress
//文件上传到HDFS中
public class FileCopyWithProgress {
	public static void main(String[] args) throws Exception {
		String localSrc = "F:\\zxb\\hadoop\\MapReuceTest\\input\\sample.txt";
		String dst = "hdfs://192.168.7.128:9000/user/root/output/sample.txt";

		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst), new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});

		IOUtils.copyBytes(in, out, 4096, true);
	}
}
// ^^ FileCopyWithProgress
