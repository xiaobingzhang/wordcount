package newwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionByText extends Partitioner<StringPair, IntWritable> {
	// 设置好划分函数可以设置输入时候的key ---value
	@Override
	public int getPartition(StringPair key, IntWritable value, int numPartitions) {
		return (key.getFirstKey().hashCode() & Integer.MAX_VALUE)
				% numPartitions;
	}

}
