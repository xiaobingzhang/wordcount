package ch4;

// cc SequenceFileReadDemo Reading a SequenceFile
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

// vv SequenceFileReadDemo
public class SequenceFileReadDemo {

	public static void main(String[] args) throws IOException {
		String uri = "1.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);

		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			Writable val = (Writable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key, val)) {
				String syncSeen = reader.syncSeen() ? "*" : "|";
				System.out.printf("[%s-%s]\t-%s\t%s\n", position, syncSeen,
						key, val);
				position = reader.getPosition(); // beginning of next record
			}
			System.out.println("-===============================================-");
			reader.seek(359);
			System.out.println(reader.next(key,val));
			//reader.seek(358);
			//System.out.println(reader.next(key,val));//throw java.lang.IndexOutOfBoundsException
			// reader.seek(360);
			// System.out.println(reader.next(key,val));//throw java.io.EOFException
			System.out.println("--------------------------------------------------");
			reader.sync(360);
			System.out.println(reader.getPosition());
			System.out.println(reader.next(key,val));//throw java.io.EOFException
			System.out.println(key);
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
// ^^ SequenceFileReadDemo