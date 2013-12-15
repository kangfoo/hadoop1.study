package com.kangfoo.study.hadoop1;

import junit.framework.TestCase;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
/**
 * Unit test for simple App.
 */
public class HDFSTest extends TestCase {

	private static String hdfsURL = "hdfs://master11:9000";

	/**
	 * <pre>
	 * 可能出现的错误： 
	 * 1. Permission denied
	 * 有两个方法离开这种安全模式
	 * （1）修改dfs.safemode.threshold.pct为一个比较小的值，缺省是0.999。
	 * （2）hadoop dfsadmin -safemode leave命令强制离开
	 * 2. Name node is in safe mode
	 * 解决办法：added this entry to conf/hdfs-site.xml
		<property>
			<name>dfs.permissions</name>
			<value>false</value>
		</property>
		或者直接把登录的用户名改成你的Hadoop集群运行hadoop的那个用户。
	 </pre>
	 * @throws IOException
	 */
	@Test
	public void testHDFSMkdir() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfsURL), conf);
		Path path = new Path("/test");
		fs.mkdirs(path);
	}

}
