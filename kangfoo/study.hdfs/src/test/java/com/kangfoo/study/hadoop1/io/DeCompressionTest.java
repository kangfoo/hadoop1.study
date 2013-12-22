package com.kangfoo.study.hadoop1.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * 
 * export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/hadoop/env/hadoop/lib/native/Linux-amd64-64:/usr/local/lib
 * mvn test -Dtest=com.kangfoo.study.hadoop1.io.DeCompressionTest
 * 
 * @date 20131222
 * @author kangfoo-mac
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DeCompressionTest {

	private String synappyPath = "/home/hadoop/env/hadoop/docs/releasenotes.html.snappy";
	private String defaultPath = "/home/hadoop/env/hadoop/docs/releasenotes.html.deflate";
	private String gzipPath = "/home/hadoop/env/hadoop/docs/releasenotes.html.gz";

	private static Configuration conf = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		conf = new Configuration();
	}

	@Test
	public void test1_SynappyCompressionCodec() throws ClassNotFoundException, IOException {
		baseDeCompressionCodec(synappyPath);
	}
	
	@Test
	public void test2_DefaultCompressionCodec() throws ClassNotFoundException, IOException {
		baseDeCompressionCodec(defaultPath);
	}
	
	@Test
	public void test3_GzipCompressionCodec() throws ClassNotFoundException, IOException {
		baseDeCompressionCodec(gzipPath);
	}

	// 
	private void baseDeCompressionCodec(String path)
			throws ClassNotFoundException, FileNotFoundException, IOException {
		CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
		CompressionCodec codec = codecFactory.getCodec(new Path(path));
		CompressionInputStream in = codec.createInputStream(new FileInputStream(new File(path)));
		FileOutputStream out = new FileOutputStream(new File(path+".decp"));
		IOUtils.copyBytes(in, out, 4096,true);
	}

}
