package ch4;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFileReaderDemo {

	public static void main(String[] args) throws IOException {
		String uri = "num.map";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		MapFile.Reader reader = null;
		
		try {
			reader = new MapFile.Reader(fs, uri, conf);
			while(reader.next(key, value))
			{
				reader.get(key, value);
				System.out.println(key+"+++++"+value.toString());
			}
		} finally{
			IOUtils.closeStream(reader);
		}
	}
}
