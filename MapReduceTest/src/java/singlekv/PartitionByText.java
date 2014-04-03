package singlekv;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionByText extends Partitioner<Text, Text> {
	// 设置好划分函数可以设置输入时候的key ---value
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		return (value.hashCode() & Integer.MAX_VALUE)
				% numPartitions;
	}

}
