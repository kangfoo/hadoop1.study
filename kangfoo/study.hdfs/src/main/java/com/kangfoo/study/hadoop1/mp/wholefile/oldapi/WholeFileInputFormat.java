/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.wholefile.oldapi;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
/**
 * 
 * 1. 没有键，用NullWritable表示。值文件的内容</br>
 * 
 * @date 2014年2月16日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class WholeFileInputFormat extends
		FileInputFormat<NullWritable, BytesWritable> {

	/**
	 * 不分片
	 */
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}
	
	@Override
	public RecordReader<NullWritable, BytesWritable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		return new WholeFileRecordReader((FileSplit)split,job);
	}

	

}