package ch5;

// cc MaxTemperatureDriverV2 Application to find the maximum temperature
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MaxTemperatureDriverV2
public class MaxTemDriver extends Configured implements Tool {
	static {
		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}
	@Override
	public int run(String[] args) throws Exception {
		/*
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		 */
		Job job = new Job(getConf(), "CC");
		job.setJarByClass(getClass());
		
		//hdfs://192.168.7.201:9000/user/root/input1/ hdfs://192.168.7.201:9000/user/root/output/
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.7.201:9000/user/root/input1/"));
		
		Path outDir =new  Path("hdfs://192.168.7.201:9000/user/root/output/");
		FileSystem fs = outDir.getFileSystem(getConf());
		if(fs.exists(outDir)){
			fs.delete(outDir, true);
		}
		
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.7.201:9000/user/root/output/"));
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		
		
		/*****************************************/
		Job job1 = new Job(getConf(), "CC");
		job1.setJarByClass(getClass());
		job1.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job1, new Path("hdfs://192.168.7.201:9000/user/root/output/part-r-00000"));
		Path outDir1 =new  Path("hdfs://192.168.7.201:9000/user/root/output1/");
		FileSystem fs1 = outDir1.getFileSystem(getConf());
		if(fs1.exists(outDir1)){
			fs1.delete(outDir1, true);
		}
		
		FileOutputFormat.setOutputPath(job1, outDir1);
		
		job1.setOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		
		job1.setMapperClass(MaxValueMapper.class);
		job1.setReducerClass(MaxValueReducer.class);

		//job1.setOutputKeyClass(Text.class);

		return job1.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemDriver(), args);
		System.exit(exitCode);
	}
}
// ^^ MaxTemperatureDriverV2
