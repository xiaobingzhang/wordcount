package stjoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class STJoinJobDriver extends Configured implements Tool {
	/*
	 * 单表关联：
	 * 
	 * 描述：
	 * 
	 * 单表的自连接求解问题。如下表，根据child-parent表列出grandchild-grandparent表的值。
	 * 
	 *child parent
		Tom Lucy
		Tom Jim
		Lucy David
		Lucy Lili
		Jim Lilei
		Jim SuSan
		Lily Green
		Lily Bians
		Green Well
		Green MillShell
		Havid James
		James LiT
		Richard Cheng
		Cheng LiHua
	 * 
	 * 问题分析：
	 * 
	 * 显然需要分解为左右两张表来进行自连接，而左右两张表其实都是child-parent表，
	 * 通过parent字段做key值进行连接。结合MapReduce的特性，
	 * MapReduce会在shuffle过程把相同的key放在一起传到Reduce进行处理。
	 * OK，这下有思路了，将左表的parent作为key输出，将右表的child做为key输出，
	 * 这样shuffle之后很自然的，左右就连接在一起了，有木有！然后通过对左右表进行求迪卡尔积便得到所需的数据。
	 */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Job job = new Job(getConf(), "STJoinJobDriver");
		job.setJarByClass(STJoinJobDriver.class);
		
		job.setMapperClass(STJoinMapper.class);
		job.setReducerClass(STJoinReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		new Path(args[1]).getFileSystem(getConf()).delete(new Path(args[1]),
				true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new STJoinJobDriver(), args);
		System.exit(exitCode);
	}

}
