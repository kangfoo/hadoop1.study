/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.sort;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @date 2014年2月22日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class TestTotalOrderPartitioner {

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

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TestTotalOrderPartitioner <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "TestTotalOrderPartitioner");

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setJarByClass(TestTotalOrderPartitioner.class);// 启动主函数类
		job.setMapperClass(Mapper1.class);

		job.setReducerClass(Reducer1.class);
		// job.setPartitionerClass(Partitioner1.class);
		// job.setNumReduceTasks(3);// 3个Reduce

		job.setPartitionerClass(TotalOrderPartitioner.class);// 使用采样器产生的文件；
		InputSampler.RandomSampler<LongWritable, NullWritable> sampler = new InputSampler.RandomSampler<LongWritable, NullWritable>(
				0.1, 10000, 10);

		Path input = FileInputFormat.getInputPaths(job)[0];
		input = input.makeQualified(input.getFileSystem(conf));
		Path partitionFile = new Path(input, "_partitions");

		System.out.println(partitionFile+"\txxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		InputSampler.writePartitionFile(job, sampler);

		// Add to DistributedCache
		URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
		System.out.println(partitionUri+"\txxxxxxxxxxxxxxxxxxxxxxxxxxxx");

		DistributedCache.addCacheFile(partitionUri, conf);
		DistributedCache.createSymlink(conf);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
