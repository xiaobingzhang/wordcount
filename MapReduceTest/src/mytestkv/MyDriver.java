package mytestkv;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MyDriver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Job job = new Job(getConf(),"MyDriver");
		job.setJarByClass(getClass());
		//inputformat default为TextInputFormat此处设置为KeyValueTextInputFormat
		//KVText默认的分割为tab，可以通过设置mapreduce.input.keyvaluelinerecordreader.key.value.separator
		
		
		//job.setInputFormatClass(KeyValueTextInputFormat.class);
	
		//使用下面这句话可以替换job.setInputFormatClass----FileInputFormat.addInputPath---job.setMapperClass
		//并且可以设置 MapReduce jobs具有多个不同的输入路径并且每个任务有不同的{@link InputFormat} and {@link Mapper}er 
		MultipleInputs.addInputPath(job, new Path(args[0]), KeyValueTextInputFormat.class, MyMapper.class);
		/*
		MultipleInputs.addInputPath(job, new Path(args[0]), KeyValueTextInputFormat.class);
		This is only useful when you have one mapper but multiple input formats.
		*/
		new Path(args[1]).getFileSystem(getConf()).delete(new Path(args[1]),true);
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	
		return job.waitForCompletion(true) ? 0 :1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MyDriver(), args);
		System.exit(exitCode);
	}
}
