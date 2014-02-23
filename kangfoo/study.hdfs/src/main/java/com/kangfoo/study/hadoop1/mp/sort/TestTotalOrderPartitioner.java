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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * ./bin/hadoop jar study.hdfs-0.0.1-SNAPSHOT.jar TestTotalOrderPartitioner   /test/sort/input  /test/sort/output1
 * 
 * 
错误：

java.lang.IllegalArgumentException: Can't read partitions file
	at org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner.setConf(TotalOrderPartitioner.java:116)
	at org.apache.hadoop.util.ReflectionUtils.setConf(ReflectionUtils.java:62)
	at org.apache.hadoop.util.ReflectionUtils.newInstance(ReflectionUtils.java:117)
	at org.apache.hadoop.mapred.MapTask$NewOutputCollector.<init>(MapTask.java:676)
	at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:756)
	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:364)
	at org.apache.hadoop.mapred.Child$4.run(Child.java:255)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:415)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1190)
	at org.apache.hadoop.mapred.Child.main(Child.java:249)
Caused by: java.io.FileNotFoundException: File _partition.lst does not exist.
	at org.apache.hadoop.fs.RawLocalFileSystem.getFileStatus(RawLocalFileSystem.java:402)
	at org.apache.hadoop.fs.FilterFileSystem.getFileStatus(FilterFileSystem.java:255)
	at org.apache.hadoop.fs.FileSystem.getLength(FileSystem.java:816)
	at org.apache.hadoop.io.SequenceFile$Reader.<init>(SequenceFile.java:1479)
	at org.apache.hadoop.io.SequenceFile$Reader.<init>(SequenceFile.java:1474)
	at org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner.readPartitions(TotalOrderPartitioner.java:301)
	at org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner.setConf(TotalOrderPartitioner.java:88)
	... 10 more
	
 * 
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

		job.setNumReduceTasks(4);
		job.setReducerClass(Reducer1.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);// 使用采样器
		InputSampler.RandomSampler<LongWritable, NullWritable> sampler = new InputSampler.RandomSampler<LongWritable, NullWritable>(
				0.1, 10000, 10);

		Path input = FileInputFormat.getInputPaths(job)[0];//得到输入路径
		input = input.makeQualified(input.getFileSystem(conf));//生成输入路径的对象
		Path partitionFile = new Path(input, "_partitions");// 目录/名字

		System.out.println(partitionFile+"\txxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);//设置排序文件所存放的路径
		InputSampler.writePartitionFile(job, sampler);//开始采样。写一个Sampler提供的分区文件

		// Add to DistributedCache //一般都将该文件做distribute cache处理  
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
