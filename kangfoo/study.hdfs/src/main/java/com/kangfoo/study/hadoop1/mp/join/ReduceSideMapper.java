/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @date 2014年2月21日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class ReduceSideMapper extends
		Mapper<LongWritable, Text, IntWritable, EmpDep> {

	private EmpDep empDep = new EmpDep();

	/**
	 * 一行行读取，并标记
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split("\\s+");

		if (values.length == 4) {// 数据在 Emp 表
			empDep.setName(values[0]);
			empDep.setSex(values[1]);
			empDep.setAge(Integer.parseInt(values[2]));
			empDep.setDepNo(Integer.parseInt(values[3]));
			empDep.setTable("EMP");

			context.write(new IntWritable(Integer.parseInt(values[3])), empDep);
		}

		if (values.length == 2) {// 数据在 Dep 表
			empDep.setDepNo(Integer.parseInt(values[0]));
			empDep.setDepName(values[1]);
			empDep.setTable("DEP");

			context.write(new IntWritable(Integer.parseInt(values[0])), empDep);
		}
	}
}
