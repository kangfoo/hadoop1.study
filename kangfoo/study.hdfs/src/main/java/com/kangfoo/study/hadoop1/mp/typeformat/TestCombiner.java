/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.typeformat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.kangfoo.study.hadoop1.mp.typeformat.TestMapreduceTextInputFormat.IntSumReducer;

/**
 * @date 2014年2月17日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class TestCombiner {

	public static class Mapper1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	/**
	 * 统计词频，key 相同的 value ++ combiner等同于reduce
	 * 
	 * @date 2014年2月17日
	 * @author kangfoo-mac
	 * @version 1.0.0
	 */
	public static class Combiner1 extends
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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: TestCombiner <in> <out> <是否启用reduce,true/false>");
			System.exit(2);
		}
		Job job = new Job(conf, "TestCombiner");
		job.setJarByClass(TestCombiner.class);// 启动主函数类
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Combiner1.class);
		
		if(Boolean.parseBoolean(args[2])){
			job.setReducerClass(IntSumReducer.class);
		}else{
			// job.setReducerClass(IntSumReducer.class); 不启用 reduce
			job.setNumReduceTasks(0); //不执行 reduce 过程
		}
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
