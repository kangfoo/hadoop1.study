/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.counter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * @date 2014年2月20日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class TestCounter {

	/**
	 * 将一行分词之后，小于2个单词，或者多于2个单词的，用计数器进行统计。
	 * @date 2014年2月20日
	 * @author kangfoo-mac
	 * @version 1.0.0
	 */
	public static class Mapper1 extends Mapper<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] val = value.toString().split("\\s+");
			if (val.length<2) {
				context.getCounter("ERROR_COUNTER","BELOW_2").increment(1);;
			}else if(val.length>2){
				context.getCounter("ERROR_COUNTER","Above_2").increment(1);;				
			}
			context.write(key, value);
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
		job.setJarByClass(TestCounter.class);// 启动主函数类
		job.setMapperClass(Mapper1.class);
		job.setNumReduceTasks(0); // 不启用reducer

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
