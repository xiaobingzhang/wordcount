package newwritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringPair implements WritableComparable<StringPair> {
	private String firstKey;
	private int secondKey;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(firstKey);
		out.writeInt(secondKey);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstKey = in.readUTF();
		secondKey = in.readInt();
	}

	@Override
	public int compareTo(StringPair o) {
		return o.firstKey.compareTo(this.firstKey);
	}

	public int getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(int secondKey) {
		this.secondKey = secondKey;
	}

	public String getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(String firstKey) {
		this.firstKey = firstKey;
	}

}
