/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.wholefile.oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.kangfoo.study.hadoop1.JobBuilder;

/**
 * @date 2014年2月17日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class SmallFilesToSequenceFileConverter extends Configured implements
		Tool {

	static class SequenceFileMapper extends MapReduceBase implements Mapper<NullWritable, BytesWritable, Text, BytesWritable>{
		
		private JobConf conf;

		@Override
		public void map(NullWritable key, BytesWritable value,
				OutputCollector<Text, BytesWritable> output, Reporter reporter)
				throws IOException {
			String filename = conf.get("map.input.file");
			output.collect(new Text(filename), value);
		}
		
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (conf==null) {
			return -1;
		}
		
		conf.setInputFormat(WholeFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(BytesWritable.class);
		
		conf.setMapperClass(SequenceFileMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		
		JobClient.runJob(conf);
		
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
		System.exit(exitCode);
	}

}
