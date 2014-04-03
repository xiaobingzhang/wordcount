package newwritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextIntComparator extends WritableComparator {

	public TextIntComparator() {
		super(StringPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		StringPair sp1 = (StringPair) a;
		StringPair sp2 = (StringPair) b;
		if (!sp1.getFirstKey().equalsIgnoreCase(sp2.getFirstKey())) {
			return sp1.getFirstKey().compareTo(sp2.getFirstKey());
		} else {
			return sp1.getSecondKey() - sp2.getSecondKey();
		}
	}
}
