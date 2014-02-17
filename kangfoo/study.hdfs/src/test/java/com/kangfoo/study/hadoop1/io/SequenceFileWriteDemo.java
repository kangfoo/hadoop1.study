/**
 * 
 */
package com.kangfoo.study.hadoop1.io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * @date 2014年2月9日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class SequenceFileWriteDemo {
	
	private static final String[] DATA = { 
			"One, two, buckle my shoe",
			"Three, four, shut the door", 
			"Five, six, pick up sticks",
			"Seven, eight, lay them straight", 
			"Nine, ten, a big fat hen" };

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		//String uri = args[0];
		String uri = "hdfs://master11:9000/numbers.seq";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		
		try {
			// 1. 创建SequenceFile.Writer对象
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			for (int i = 0; i < 100; i++) {
				key.set(100 - i); // key 100到1倒序存储。
				value.set(DATA[i % DATA.length]); // value
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);// writer.getLength() 获取当前存储的位置
				writer.append(key, value); // 添加到尾部
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

}
