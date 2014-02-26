/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.examples;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;

public class WordCount2 {

	public static void main(String[] args) throws Exception {

		JobClient client = new JobClient();
		JobConf conf = new JobConf(WordCount2.class);
		
		FileInputFormat.addInputPath(conf, new Path("hdfs://192.168.7.128:9000/user/root/input"));
		FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.7.128:9000/user/root/output"));
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setMapperClass(TokenCountMapper.class);
		conf.setCombinerClass(LongSumReducer.class);
		conf.setReducerClass(LongSumReducer.class);
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
