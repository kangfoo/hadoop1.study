/**
 * 
 */
package com.kangfoo.study.hadoop1.mp.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 在Mapper端join
 * 
 * @date 2014年2月21日
 * @author kangfoo-mac
 * @version 1.0.0
 */
public class MapSideMapper extends
		Mapper<LongWritable, Text, NullWritable, EmpDep> {

	private Map<Integer,String> joinData = new HashMap<Integer, String>();
	
	/**
	 * 在Mapper处理之前执行。
	 * 获取文件
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// 获取路径
		Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		// 读取第一个文件(部门信息)
		BufferedReader reader =  new BufferedReader(new FileReader(paths[0].toString()));
		String str = null;
		// 读取一行
		while( (str = reader.readLine())!=null ){
			String[] s = str.split("\\s+");
			joinData.put(Integer.parseInt(s[0]), s[1]);
		}
				
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split("\\s+");
		
		EmpDep empDep = new EmpDep();
		empDep.setName(values[0]);
		empDep.setSex(values[1]);
		empDep.setAge(Integer.parseInt(values[2]));
		
		int depNo = Integer.parseInt(values[3]);
		empDep.setDepName(joinData.get(depNo));

		context.write(NullWritable.get(), empDep);
	}

}
