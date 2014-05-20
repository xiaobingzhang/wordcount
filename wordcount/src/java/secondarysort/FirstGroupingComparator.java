package secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FirstGroupingComparator extends WritableComparator {
	public FirstGroupingComparator() {
		super(IntPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntPair o1 = (IntPair) a;
		IntPair o2 = (IntPair) b;
		int l = o1.getFirst();
		int r = o2.getFirst();
		return l == r ? 0 : (l < r ? -1 : 1);

	}
}
