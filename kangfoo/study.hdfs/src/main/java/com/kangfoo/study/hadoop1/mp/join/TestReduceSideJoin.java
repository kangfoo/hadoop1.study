/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @date 2014年2月21日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class TestReduceSideJoin {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TestReduceSideJoin <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Reduce side Join");
		job.setJarByClass(TestReduceSideJoin.class);
		job.setMapperClass(ReduceSideMapper.class);
		job.setReducerClass(ReduceSideReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(EmpDep.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(EmpDep.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
