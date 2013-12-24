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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SequenceFileTest {

	private static Configuration conf = null;
	private static String hdfsURL = "hdfs://master11:9000";
	private static FileSystem fs = null;
	private static String[] data={"one, two","three, four","five, six","seven, eight","nine, ten"}; 
	private static Path path1 = null;
	private static Path path2 = null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		path1 = new Path("/tmp1.seq");
		path2 = new Path("/tmp2.seq");

		conf = new Configuration();
		fs = FileSystem.get(URI.create(hdfsURL), conf);
	}

	@Test
	public void test1_Writer() throws IOException {
		IntWritable key = new IntWritable();
		Text value = new Text();
	
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path1, key.getClass(), value.getClass());
		for(int i = 0; i<100; i++){
			key.set(100-i);
			value.set(data[i%data.length]);
			writer.append(key, value);
		}
		IOUtils.closeStream(writer);
	}	
	
	@Test
	public void test2_Reader() throws IOException {
		reader(path1);
	}
	
	@Test
	public void test3_Writer() throws IOException {
		IntWritable key = new IntWritable();
		Text value = new Text();
	
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path2, key.getClass(), value.getClass(), CompressionType.RECORD, new BZip2Codec());
		
		for(int i = 0; i<100; i++){
			key.set(100-i);
			value.set(data[i%data.length]);
			writer.append(key, value);
		}
		IOUtils.closeStream(writer);
	}	
	
	@Test
	public void test4_Reader() throws IOException {
		reader(path2);
	}

	private void reader(Path path) throws IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		while(reader.next(key,value)){
			System.out.print("key:" + key);
			System.out.print("\t" + "value:" + value);
			System.out.println("\t" + "position:" + reader.getPosition());
		}
		IOUtils.closeStream(reader);
	}
	
	/**
	 *  根据情况执行
	 * @throws IOException
	 */
	@AfterClass
	public static void test99_HDFSDeleteDir() throws IOException {
		fs.delete(path1, true);
		fs.delete(path2, true);
	}
}
