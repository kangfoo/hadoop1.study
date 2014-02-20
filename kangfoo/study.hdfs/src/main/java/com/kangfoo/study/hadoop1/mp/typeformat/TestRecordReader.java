package com.kangfoo.study.hadoop1.mp.typeformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

/**
 * 自定义 RecordReader
 * 
 * @date 2014年2月20日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class TestRecordReader {

	public static class Mapper1 extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);// 输出
		}

	}

	public static class Partitioner1 extends Partitioner<LongWritable, Text> {

		/**
		 * 指定分区
		 */
		@Override
		public int getPartition(LongWritable key, Text value, int numPartitions) {
			if (key.get() % 2 == 0) {
				key.set(0);// 偶素行，key 以 0 表示
				return 0;
			} else {
				key.set(1);// 奇数行，key 以 1 表示
				return 1;
			}
		}

	}

	/**
	 * 输出奇偶总和，累加
	 * 奇数行以1表示，
	 * 偶数行以0表示。
	 * 
	 * @date 2014年2月20日
	 * @author kangfoo-mac
	 * @version 1.0.0
	 */
	public static class Reducer1 extends
			Reducer<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			int sum =0;
			for(Text val : value ){
				sum += Integer.parseInt(val.toString());
			}
			
			Text writableKey = new Text();
			IntWritable writableValue = new IntWritable();
			
			if (key.get()==0) {
				writableKey.set("偶素行之和：");
			}else{
				writableKey.set("奇数行之和：");
			}
			writableValue.set(sum);
			context.write(writableKey, writableValue);
		}
	}

	/**
	 * 类型同RecordReader1
	 * 
	 * @date 2014年2月20日
	 * @author kangfoo-mac
	 * @version 1.0.0
	 */
	public static class FileInputFormat1 extends
			FileInputFormat<LongWritable, Text> {
		@Override
		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			return new RecordReader1();
		}

		/**
		 * 不分割
		 */
		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
			return false;
		}
	}

	/**
	 * 具体可参考 LineRecordReader
	 * 
	 * @date 2014年2月20日
	 * @author kangfoo-mac
	 * @version 1.0.0
	 */
	public static class RecordReader1 extends RecordReader<LongWritable, Text> {

		private long start; // 偏移量
		private long pos; // 行号
		private long end; // 当前分片，在整个文件的结束位置
		private FSDataInputStream fin = null; //
		private LongWritable key = null; // 行号
		private Text value = null; // 文本
		private LineReader reader = null;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) split;
			start = fileSplit.getStart();
			end = start + fileSplit.getLength();

			Configuration conf = context.getConfiguration();
			Path path = fileSplit.getPath();
			FileSystem fs = path.getFileSystem(conf);
			fin = fs.open(path);
			fin.seek(start);// 调整指针，指向start偏移量
			reader = new LineReader(fin);
			pos = 1;// 第一行
		}

		/**
		 * 未考虑跨分片
		 */
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (key == null) {
				key = new LongWritable();
			}
			key.set(pos);

			if (value == null) {
				value = new Text();
			}
			if (reader.readLine(value) == 0) {
				return false;
			}
			pos++;
			return true;

		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void close() throws IOException {
		}

	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: TestRecordReader <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "TestRecordReader");
	    job.setJarByClass(TestRecordReader.class);//启动主函数类
	    job.setMapperClass(Mapper1.class);
	    
	    job.setReducerClass(Reducer1.class);
	    job.setPartitionerClass(Partitioner1.class);
	    job.setNumReduceTasks(2);// 启动2个reduceTask
	    
	    job.setInputFormatClass(FileInputFormat1.class);
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

	
}
