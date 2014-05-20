package mysort;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MySortDriver extends Configured implements Tool {
	private final static Log log = LogFactory.getLog(MySortDriver.class);
	static {
		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Job job = new Job(getConf(), "MySortDriver");
		job.setJarByClass(getClass());
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		new Path(args[1]).getFileSystem(getConf()).delete(new Path(args[1]),
				true);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job.setPartitionerClass(TotalOrderPartitioner.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(MySortMapper.class);
		job.setReducerClass(MySortReducer.class);

		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		
		int res =  job.waitForCompletion(true) ? 0 : 1;
		Counters counters = job.getCounters();
		Counter c1 = counters.findCounter(ValEnum.LESSTHAN100);
		log.info("c1.getValue())"+c1.getValue());
		log.info("c1.getDisplayName())"+c1.getDisplayName()+",c1.getName()"+c1.getName());
		Counter c2 = counters.findCounter(ValEnum.MORETHAN100);
		log.info("c2.getValue())"+c2.getValue());
		log.info("c2.getDisplayName())"+c2.getDisplayName()+",c2.getName()"+c2.getName());
		
		return res;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MySortDriver(), args);
		System.exit(exitCode);
	}
}
