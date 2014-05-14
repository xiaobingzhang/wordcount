package ch5;

// cc MaxTemperatureMapperV1 First version of a Mapper that passes MaxTemperatureMapperTest
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

//vv MaxTemperatureMapperV1
public class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(MaxTemperatureMapper.class);
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//1991-32
		String line = value.toString();
			String year = line.substring(0, 4);
			int airTemperature = Integer.parseInt(line.substring(5));
			LOG.info(year+ airTemperature);
			context.write(new Text(year), new IntWritable(airTemperature));
	}
}
// ^^ MaxTemperatureMapperV1
