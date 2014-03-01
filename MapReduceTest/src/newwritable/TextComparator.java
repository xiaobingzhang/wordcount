package newwritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextComparator extends WritableComparator {

	public TextComparator() {
		super(StringPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	//设置group函数
	public int compare(WritableComparable a, WritableComparable b) {
		StringPair sp1 = (StringPair) a;
		StringPair sp2 = (StringPair) b;
		return sp1.getFirstKey().compareTo(sp2.getFirstKey());
	}
}
