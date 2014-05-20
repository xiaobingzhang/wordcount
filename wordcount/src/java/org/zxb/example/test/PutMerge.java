package org.zxb.example.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

public class PutMerge {
	public static void main(String[] args) {

		String argsArray[] = {"C:\\Users\\zxb\\Desktop\\11","tmp1"};
		Configuration conf = new Configuration();
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(conf);
			FileSystem local = FileSystem.getLocal(conf);

			Path inputDir = new Path(argsArray[0]);
			Path hdfsFile = new Path(argsArray[1]);

			FileStatus[] inputFile = local.listStatus(inputDir);
			FSDataOutputStream out = hdfs.create(hdfsFile);

			for (int i = 0; i < inputFile.length; i++) {
				System.out.println(inputFile[i].getPath().getName());
				FSDataInputStream in = local.open(inputFile[i].getPath());
				byte buffer[] = new byte[256];
				int byteRead = 0;
				while ((byteRead = in.read(buffer)) > 0) {
					out.write(buffer, 0, byteRead);
				}
				in.close();
			}
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
