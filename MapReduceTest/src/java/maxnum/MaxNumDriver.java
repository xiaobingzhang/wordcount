package maxnum;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxNumDriver extends Configured implements Tool {
	/*
	static {
		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}
	*/
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
		return -1;
		}
		
		Job job = new Job(getConf(),"Max Number");
		job.setJarByClass(MaxNumDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaxNumMapper.class);
		job.setCombinerClass(MaxNumReducer.class);
		job.setReducerClass(MaxNumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		return job.waitForCompletion(true) ? 0 :1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxNumDriver(), args);
		System.exit(exitCode);
	}
}
