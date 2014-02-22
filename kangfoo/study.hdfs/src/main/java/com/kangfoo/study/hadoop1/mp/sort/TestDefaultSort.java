/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * 使用Hadoop默认的shuff机制，对数据key进行排序。按key的区间值进行分区。
 * 
 * @date 2014年2月22日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class TestDefaultSort {

	public static class Mapper1 extends
			Mapper<LongWritable, Text, LongWritable, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split("\\s+");
			context.write(new LongWritable(Long.parseLong(values[0])),
					NullWritable.get());
		}
	}

	public static class Reducer1 extends
			Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<NullWritable> value,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static class Partitioner1 extends
			Partitioner<LongWritable, NullWritable> {

		@Override
		public int getPartition(LongWritable key, NullWritable value,
				int numPartitions) {
			if (key.get() < 100) {
				return 0 % numPartitions;
			}
			if (key.get() >= 100 && key.get() < 1000) {
				return 1 % numPartitions;
			}

			return 2 % numPartitions;

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TestSort <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "TestSort");
		job.setJarByClass(TestDefaultSort.class);// 启动主函数类
		job.setMapperClass(Mapper1.class);

		job.setReducerClass(Reducer1.class);
		job.setPartitionerClass(Partitioner1.class);
		job.setNumReduceTasks(3);// 3个Reduce

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
