package secondarysort;

import org.apache.hadoop.io.WritableComparator;

public class IntPairComparator extends WritableComparator {
	public IntPairComparator() {
		super(IntPair.class);
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return compareBytes(b1, s1, l1, b2, s2, l2);
	}
}
