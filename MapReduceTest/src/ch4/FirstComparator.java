package ch4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class FirstComparator extends WritableComparator {
	public static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

	public FirstComparator() {
		super(TextPair.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
					+ readVInt(b1, s1);// compute the real length of the input
										// byte[]
			int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
					+ readVInt(b2, s2);
			int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
			return cmp;
		} catch (IOException e) {
			throw new IllegalArgumentException();
		}
	}
	
	@Override
	public int compare(Object a, Object b) {
		if(a instanceof TextPair && b instanceof TextPair)
		{
			TextPair aTmp = (TextPair)a;
			TextPair bTmp = (TextPair)b;
			return aTmp.getFirst().compareTo(bTmp.getFirst());
		}
		return super.compare(a, b);
	}
}
