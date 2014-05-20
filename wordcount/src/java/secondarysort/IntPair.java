package secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntPair implements WritableComparable<IntPair> {
	private int first = 0;
	private int second = 0;

	public void set(int left, int right) {
		first = left;
		second = right;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first - Integer.MIN_VALUE);
		out.writeInt(second - Integer.MIN_VALUE);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt() + Integer.MIN_VALUE;
		second = in.readInt() + Integer.MIN_VALUE;
	}

	@Override
	public int hashCode() {
		return first * 157 + second;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof IntPair)) {
			return false;
		}
		IntPair r = (IntPair) o;
		return r.first == first && r.second == second;
	}

	static {
		WritableComparator.define(IntPair.class, new IntPairComparator());
	}

	@Override
	public int compareTo(IntPair o) {
		if (first != o.first) {
			return first < o.first ? -1 : 1;
		} else if (second != o.second) {
			return second < o.second ? -1 : 1;
		} else {
			return 0;
		}
	}

	public int getFirst() {
		return first;
	}

	public int getSecond() {
		return second;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public void setSecond(int second) {
		this.second = second;
	}

}
