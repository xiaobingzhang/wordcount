package ch4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {
	private Text first;
	private Text second;
	public TextPair() {
		set(new Text(), new Text());
	}

	public TextPair(Text first, Text second) {
		set(first, second);
	}

	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TextPair) {
			TextPair tp = (TextPair) obj;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	public String toString() {
		return first + " " + second;
	}

	@Override
	public int compareTo(TextPair o) {
		
		int cmp = first.compareTo(o.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(o.second);
	}

}
