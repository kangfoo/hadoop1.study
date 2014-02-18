/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.typeformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @date 2014年2月18日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class TestPartitioner {

	/**
	 * 同 wordcount
	 * 
	 * @date 2014年2月18日
	 * @author kangfoo-mac
	 * @version 1.0.0
	 */
	public static class Mapper1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] s = value.toString().split("\\s+");

			word.set(s[0]);
			one.set(Integer.parseInt(s[1]));
			context.write(word, one);
		}

	}

	/**
	 * value 累加
	 * 
	 * @date 2014年2月18日
	 * @author kangfoo-mac
	 * @version 1.0.0
	 */
	public static class Reducer1 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	/**
	 * 假设4个reduce. 不同的商品根据商品名称分发到指定的reduce。
	 * 
	 * @date 2014年2月18日
	 * @author kangfoo-mac
	 * @version 1.0.0
	 */
	public static class Partitioner1 extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			if (key.toString().equals("shoes")) // 转发给4个不同的reducer
				return 0;
			if (key.toString().equals("hat"))
				return 1;
			if (key.toString().equals("stockings"))
				return 2;
			return 3;
		}

	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TestPartitioner <in> <out> ");
			System.exit(2);
		}
		Job job = new Job(conf, "TestPartitioner");
		job.setJarByClass(TestPartitioner.class);// 启动主函数类
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setPartitionerClass(Partitioner1.class);
		job.setNumReduceTasks(4); // 设置4个reducer

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
