package singlekv;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SingleKVJobDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = new Job(getConf(), "SingleKVJobDriver");
		job.setJarByClass(getClass());
		new Path(args[1]).getFileSystem(getConf()).delete(new Path(args[1]),
				true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setPartitionerClass(PartitionByText.class);

		job.setMapperClass(SingleKVMapper.class);
		job.setCombinerClass(SingleKVReducer.class);
		job.setReducerClass(SingleKVReducer.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SingleKVJobDriver(), args);
		System.exit(exitCode);
		;
	}

}
