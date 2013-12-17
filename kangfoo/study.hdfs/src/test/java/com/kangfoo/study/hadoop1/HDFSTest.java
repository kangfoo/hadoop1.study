package com.kangfoo.study.hadoop1;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * HDFSTest
 * 
 * @author kangfoo-mac
 * 
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HDFSTest {

	private static String hdfsURL = "hdfs://master11:9000";
	private static Configuration conf = null;
	private static FileSystem fs = null;

	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	@BeforeClass
	public static void init() throws IOException {
		conf = new Configuration();
		fs = FileSystem.get(URI.create(hdfsURL), conf);
	}

	/**
	 * <pre>
	 * 可能出现的错误： 
	 * 1. Permission denied
	 * 有两个方法离开这种安全模式
	 * （1）修改dfs.safemode.threshold.pct为一个比较小的值，缺省是0.999。
	 * （2）hadoop dfsadmin -safemode leave命令强制离开
	 * 2. Name node is in safe mode
	 * 解决办法：added this entry to conf/hdfs-site.xml
	 * 		<property>
	 * 			<name>dfs.permissions</name>
	 * 			<value>false</value>
	 * 		</property>
	 * 		或者直接把登录的用户名改成你的Hadoop集群运行hadoop的那个用户。
	 * </pre>
	 * 
	 * @throws IOException
	 */
	@Test
	public void test1_HDFSMkdir() throws IOException {
		Path path = new Path("/test");
		fs.mkdirs(path);

		Assert.assertEquals("文件夹已建立", true, fs.exists(path));
	}

	@Test
	public void test2_CreateFile() throws IOException {
		FSDataInputStream in = null;
		FSDataOutputStream out = null;
		
		try {
			String s = "hello hadoop";

			Path path = new Path("/test/a.txt");
			out = fs.create(path);
			out.write(s.getBytes());

			// 为什么读不到呢？Thead.sleep(20000);也无效。why?
			byte[] bytes = new byte[1024];
			in = fs.open(path);

			int readLen = in.read(bytes);
			while (-1 != readLen) {
				System.out.println(new String(bytes));
				readLen = in.read(bytes);
			}

			// Assert.assertSame("hello hadoo写入成功", s, sb.toString());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}

	/**
	 * 读数据
	 * @throws IOException
	 */
	@Test
	public void test3_ReadFile() throws IOException{
		Path path = new Path("/test/a.txt");
		FSDataInputStream in = null;
					byte[] bytes = new byte[1024];
					in = fs.open(path);

					int readLen = in.read(bytes);
					while (-1 != readLen) {
						System.out.println(new String(bytes));
						readLen = in.read(bytes);
					}
	}
	
	@Test
	public void test4_RenameFile() throws IOException {
		Path path = new Path("/test/a.txt");
		Path newPath = new Path("/test/b.txt");
		Boolean bl = fs.rename(path, newPath);

		Assert.assertEquals("文件重命名成功", true, bl);
	}

	@Test
	public void test5_UploadLocalFile1() throws IOException {
		Path src = new Path("/Users/kangfoo-mac/study/hadoop-1.2.1/bin/rcc");
		Path dst = new Path("/test");
		fs.copyFromLocalFile(src, dst);

		Assert.assertEquals("文件已上传", true, fs.exists(new Path("/test/rcc")));
	}

	/**
	 *  dd if=/dev/zero of=data bs=1024 count=1024
	 * @throws IOException
	 */
	@Test
	public void test6_UploadLocalFile2() throws IOException {
		InputStream in = null;
		FSDataOutputStream out = null;
		try {
			in = new BufferedInputStream(new FileInputStream(new File(
					"/Users/kangfoo-mac/study/hadoop-1.2.1/bin/data")));
			out = fs.create(new Path("/test/data"), new Progressable() {

				@Override
				public void progress() {
					System.out.print(".");
				}
			});
			IOUtils.copyBytes(in, out, 1024);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}

		Assert.assertEquals("文件已上传", true, fs.exists(new Path("/test/data")));
	}

	@Test
	public void test7_ListFiles() throws IOException {
		Path dst = new Path("/test");
		FileStatus[] files = fs.listStatus(dst);

		Assert.assertNotNull(files);

		for (FileStatus f : files) {
			System.out.println(f.getPath().toString());
		}
	}

	@Test
	public void test7_GetBlockInfo() throws IOException {
		Path dst = new Path("/test/data");
		FileStatus fileStatus = fs.getFileStatus(dst);
		BlockLocation[] bl = fs.getFileBlockLocations(fileStatus, 0,
				fileStatus.getLen());

		for (BlockLocation b : bl) {

			Assert.assertNotNull(b.getHosts());

			for (int i = 0; i < b.getHosts().length; i++) {
				System.out.println(b.getHosts()[i]);
			}
		}
	}

	/**
	 *  根据情况执行
	 * @throws IOException
	 */
//	@AfterClass
//	public static void test99_HDFSDeleteDir() throws IOException {
//		Path path = new Path("/test");
//		fs.delete(path, true);
//	}

}
