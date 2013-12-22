package com.kangfoo.study.hadoop1.io;

import static org.junit.Assert.*;

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
 * @Date 2013-12-11 17:21
 * @author kangfoo-mac
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CompressionTest {

	private String synappyCodeClazz = "org.apache.hadoop.io.compress.SnappyCodec";
	private static Configuration conf = null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		conf = new Configuration();
	}

	@Test
	public void test1_SynappyCompressionCodec() throws ClassNotFoundException, IOException {
		Class<?> clzz = Class.forName(synappyCodeClazz);
		CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(clzz, conf);
		String inputFile = "/home/hadoop/env/hadoop/docs/releasenotes.html";
		String outFile = inputFile + codec.getDefaultExtension();
		
		FileOutputStream fileOut = new FileOutputStream(outFile);
		CompressionOutputStream out = codec.createOutputStream(fileOut);
		
		FileInputStream in = new FileInputStream(inputFile);
		IOUtils.copyBytes(in, out, 4096,false);
		in.close();
		out.close();
	}

}
