package newwritable;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Job job = new Job(getConf(), "JobDriver");
		job.setJarByClass(getClass());

		// 删除输出文件夹
		new Path(args[1]).getFileSystem(getConf()).delete(new Path(args[1]),
				true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		// 设置输入格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// 设置map的输出类型
		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 设置排序
		job.setSortComparatorClass(TextIntComparator.class);

		// 设置group
		job.setGroupingComparatorClass(TextComparator.class);

		job.setPartitionerClass(PartitionByText.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		long begin = System.currentTimeMillis();
		
		int exitCode = ToolRunner.run(new JobDriver(), args);
		
		long end = System.currentTimeMillis();
		System.out.println(end - begin);
		System.exit(exitCode);
	}

}
