package com.kangfoo.study.hadoop1.mp.wholefile.oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 * 
 * 1. WholeFileRecordReader负责将 FileSplit 转化成一条纪录，该纪录的键是null，值是
 * 这文件的内容。
 * 2. 因为是只有一条纪录，WholeFileRecordReader要么处理该纪录要么不处理。所以
 * 用 boolean 变量 processed 表示 是否被处理过。
 * 3. next() 方法。打开文件，产生长度是文件长度的字节数组，并把文件内容存放到数组中。
 * @date 2014年2月17日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class WholeFileRecordReader implements
		RecordReader<NullWritable, BytesWritable> {

	private FileSplit fileSplit;
	private Configuration conf;
	private boolean processed = false;
	
	
	public WholeFileRecordReader(FileSplit split, Configuration job) {
		this.fileSplit = split;
		this.conf = job;
	}

	@Override
	public boolean next(NullWritable key, BytesWritable value)
			throws IOException {
		if(!processed){
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try {
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents, 0 , contents.length);
			} finally {
				IOUtils.closeStream(in);
			}
			
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public NullWritable createKey() {
		return NullWritable.get();
	}

	@Override
	public BytesWritable createValue() {
		return new BytesWritable();
	}

	@Override
	public long getPos() throws IOException {
		return processed ? fileSplit.getLength() : 0;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

}
