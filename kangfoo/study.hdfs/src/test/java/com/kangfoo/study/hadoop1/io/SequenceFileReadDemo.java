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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @date 2014年2月9日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class SequenceFileReadDemo {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// String uri = args[0];
		String uri = "hdfs://master11:9000/numbers.seq";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		Path path = new Path(uri);
		SequenceFile.Reader reader = null;
		
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf); // 反射key类型实例
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while(reader.next(key,value)){ // 循环调用reader.next(key,value)方法
				String syncSeen = reader.syncSeen() ?"*":"";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition();
			}
			
		} finally {
			IOUtils.closeStream(reader);
		}
	}

}
