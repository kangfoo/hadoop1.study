package com.kangfoo.study.hadoop1.io;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class WritableComparableTest2 {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}
	
	private static byte[] serialize(Writable writable) throws IOException{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataout = new DataOutputStream(out);
		writable.write(dataout);
		dataout.close();
		return out.toByteArray();
	}
	
	private static byte[] deserialize(Writable writable, byte[] bytes) throws IOException{
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream datain = new DataInputStream(in);
		writable.readFields(datain);
		return bytes;
	}
	
	@Test
	public void test1() throws IOException{
		IntWritable writable = new IntWritable(163);
		byte[] bytes = WritableComparableTest2.serialize(writable);
		assertThat(bytes.length, is(4));// 占用4字节
		
		assertThat(StringUtils.byteToHexString(bytes), is("000000a3")); // 163(2) = A3(16)
	
		IntWritable writable2 = new IntWritable();
		deserialize(writable2, bytes);
		assertThat(writable2.get(), is(163));
		
		bytes = serialize(new VIntWritable(163));
		assertThat(StringUtils.byteToHexString(bytes), is("8fa3"));
	}

	@Test
	public void test2() throws IOException{
		BytesWritable b = new BytesWritable(new byte[]{2,5,127});
		byte[] bytes = serialize(b);
		assertThat(StringUtils.byteToHexString(bytes), is("0000000302057f"));
	}

	
	
}
