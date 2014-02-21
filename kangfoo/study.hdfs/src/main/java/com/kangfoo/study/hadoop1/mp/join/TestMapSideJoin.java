/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.join;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @date 2014年2月22日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class TestMapSideJoin {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in> <in2> <out>");
			System.exit(2);
		}

		DistributedCache.addCacheFile(new Path(args[1]).toUri(), conf);

		Job job = new Job(conf, "TestMapSideJoin");
		job.setJarByClass(TestMapSideJoin.class);
		job.setMapperClass(MapSideMapper.class);

		job.setNumReduceTasks(0);// 没有Reduce

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(EmpDep.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(EmpDep.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
