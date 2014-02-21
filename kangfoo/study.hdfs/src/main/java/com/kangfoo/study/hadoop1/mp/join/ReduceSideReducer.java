/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @date 2014年2月21日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class ReduceSideReducer extends
		Reducer<IntWritable, EmpDep, NullWritable, EmpDep> {

	@Override
	protected void reduce(IntWritable key, Iterable<EmpDep> values,
			Context context) throws IOException, InterruptedException {
		String depName = "";
				
		List<EmpDep> list = new LinkedList<EmpDep>();// 存储拷贝副本

		// 先找 dep no
		for (EmpDep val : values) {
			list.add(new EmpDep(val));// 拷贝副本

			if (val.getTable().equals("DEP")) {
				depName = val.getDepName();
			}
		}

		// 设置 depName
		for (EmpDep v : list) {
			if (v.getTable().equals("EMP")) {
				v.setDepName(depName);
			}
			context.write(NullWritable.get(), v);
		}
	}
}
