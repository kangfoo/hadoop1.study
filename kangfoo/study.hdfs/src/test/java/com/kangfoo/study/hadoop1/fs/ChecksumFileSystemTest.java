package com.kangfoo.study.hadoop1.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @date 20131222
 * @author kangfoo-mac
 *
 */
public class ChecksumFileSystemTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Test
	public void test() throws IOException {
		Configuration conf = new Configuration();
		LocalFileSystem localFS = ChecksumFileSystem.getLocal(conf);
		Path path = localFS.getChecksumFile(new Path("/home/hadoop/env/hadoop/docs/releasenotes.html"));
		Assert.assertNotNull(path);
		System.out.println(path);
	}

}
