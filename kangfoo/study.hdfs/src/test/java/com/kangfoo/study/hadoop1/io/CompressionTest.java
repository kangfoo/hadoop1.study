package com.kangfoo.study.hadoop1.io;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * 
 * 测试本测试用例前，请先设置环境变量，再使用maven执行test
 * 
 * <pre>
 * export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/hadoop/env/hadoop/lib/native/Linux-amd64-64:/usr/local/lib
 * mvn test -Dtest=com.kangfoo.study.hadoop1.io.CompressionTest
 * 
 * -rw-rw-r--. 1 hadoop hadoop 520K 7月  23 06:26 releasenotes.html
 * -rw-rw-r--. 1 hadoop hadoop 138K 12月 22 18:36 releasenotes.html.deflate
 * -rw-rw-r--. 1 hadoop hadoop 138K 12月 22 18:36 releasenotes.html.gz
 * -rw-rw-r--. 1 hadoop hadoop 220K 12月 22 18:36 releasenotes.html.snappy
 * </pre>
 * @Date 2013-12-11 17:21
 * @author kangfoo-mac 
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CompressionTest {

	private String synappyCodeClazz = "org.apache.hadoop.io.compress.SnappyCodec";
	private String defaultCodecClazz ="org.apache.hadoop.io.compress.DefaultCodec";
	private String gzipCodecClazz = "org.apache.hadoop.io.compress.GzipCodec";
	
	private static Configuration conf = null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		conf = new Configuration();
	}

	/**
	 *  
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	@Test
	public void test1_SynappyCompressionCodec() throws ClassNotFoundException, IOException {
		baseCompressionCodec(synappyCodeClazz);
	}
	
	@Test
	public void test2_DefaultCompressionCodec() throws ClassNotFoundException, IOException {
		baseCompressionCodec(defaultCodecClazz);
	}
	
	@Test
	public void test3_GzipCompressionCodec() throws ClassNotFoundException, IOException {
		baseCompressionCodec(gzipCodecClazz);
	}

	private void baseCompressionCodec(String clazz) throws ClassNotFoundException,
			FileNotFoundException, IOException {
		Class<?> clzz = Class.forName(clazz);
		CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(clzz, conf);
		String inputFile = "/home/hadoop/env/hadoop/docs/releasenotes.html";
		String outFile = inputFile + codec.getDefaultExtension();
		
		FileOutputStream fileOut = new FileOutputStream(outFile);
		CompressionOutputStream out = codec.createOutputStream(fileOut);
		
		FileInputStream in = new FileInputStream(inputFile);
		IOUtils.copyBytes(in, out, 4096,true);
	}

}
