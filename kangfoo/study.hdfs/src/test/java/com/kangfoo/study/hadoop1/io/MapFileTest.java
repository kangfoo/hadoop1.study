package com.kangfoo.study.hadoop1.io;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * 1. 创建小文件，小文件 合并sequence, 2. mapfile读写程序
 * 
 * @author kangfoo-mac
 * 
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MapFileTest {

	String uri = "hdfs://master11:9000/numbers.map";
	String uri2 = "hdfs://master11:9000/numbers2.map";

	private static final String[] DATA = { "One, two, buckle my shoe",
			"Three, four, shut the door", "Five, six, pick up sticks",
			"Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

	@Test
	public void test1_write() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		IntWritable key = new IntWritable();
		Text value = new Text();
		MapFile.Writer writer = null;
		try {
			writer = new MapFile.Writer(conf, fs, uri, key.getClass(),
					value.getClass());
			for (int i = 0; i < 1024; i++) {
				key.set(i + 1);
				value.set(DATA[i % DATA.length]);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
			IOUtils.closeStream(fs);
		}
	}

	@Test
	public void test2_read() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		MapFile.Reader reader = null;

		try {
			reader = new MapFile.Reader(fs, uri, conf);
			IntWritable key = (IntWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf); // 反射key类型实例
			Text value = (Text) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			while (reader.next(key, value)) { // 循环调用reader.next(key,value)方法
				System.out.printf("[%s]%s\n", key, value);
			}

		} finally {
			IOUtils.closeStream(reader);
			IOUtils.closeStream(fs);
		}
	}

	@Test
	public void test3_indexReader() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		MapFile.Reader reader = null;

		try {
			reader = new MapFile.Reader(fs, uri, conf);
			Text value = new Text();
			IntWritable key = new IntWritable(496);
			
			/*
			 * index 索引偏移量
			 * /bin/hadoop fs -text /numbers.map/index
			 * 
			 * 1	128
			 * 129	6079
			 * 257	12054
			 * 385	18030
			 * 513	24002
			 * 641	29976
			 * 769	35947
			 * 897	41922
			 * 
			 */
			reader.get(key, value);// 二分法查找。一次磁盘寻址 ＋ 一次最多顺序128（默认值等于每128下一个索引）个条目顺序扫瞄
			// System.out.printf("[%s]%s\n", key, value);
			assertThat(value.toString(), is("One, two, buckle my shoe"));

		} finally {
			IOUtils.closeStream(reader);
			IOUtils.closeStream(fs);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void test4_mapFileFixer() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri2), conf);
		Path map = new Path(uri2);
		Path mapData = new Path(map, MapFile.DATA_FILE_NAME);
		// Get key and value types from data sequence file
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, mapData, conf);
		Class keyClass = reader.getKeyClass();
		Class valueClass = reader.getValueClass();
		reader.close();
		// Create the map file index file
		long entries = MapFile.fix(fs, map, keyClass, valueClass, false, conf);
		System.out.printf("Created MapFile %s with %d entries\n", map, entries);
	}
}
