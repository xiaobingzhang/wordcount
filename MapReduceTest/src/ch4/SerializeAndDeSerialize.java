package ch4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class SerializeAndDeSerialize {
	public IntWritable writable = new IntWritable(163);

	public static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}

	public static byte[] deserialize(Writable writable, byte[] bytes)
			throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
		return bytes;
	}

	public static void main(String[] args) throws IOException {
		IntWritable a = new IntWritable(163);
		deserialize(a, serialize(a));
		System.out.println(a.get());
		System.out.println("-----------------------");
		RawComparator<IntWritable> comparator = WritableComparator
				.get(IntWritable.class);

		IntWritable w1 = new IntWritable(163);
		IntWritable w2 = new IntWritable(66);

		Integer res = comparator.compare(w1, w2);
		System.out.println(res);
		System.out.println("----------------------------");

		Text t = new Text("hadoop");
		System.out.println(t.getLength());
		System.out.println(t.getBytes().length);
		System.out.println(t.charAt(2) == (int) 'd');
		System.out.println(t.charAt(100));// out of bound
		System.out.println("----------------------------");
		
		MapWritable src = new MapWritable();
		src.put(new IntWritable(1), new Text("cat"));
		src.put(new VIntWritable(2), new LongWritable(163));
		
		MapWritable dest = new MapWritable();
		WritableUtils.cloneInto(dest, src);
		System.out.println(dest.get(new IntWritable(1)));
		System.out.println(dest.get(new VIntWritable(2)));
		System.out.println(((Text) dest.get(new IntWritable(1))).equals(new Text("cat")));
		System.out.println((LongWritable) dest.get(new VIntWritable(2)) == (new LongWritable(163)));
	}
}
