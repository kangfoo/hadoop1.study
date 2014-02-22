/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kangfoo.study.hadoop1;

import org.apache.hadoop.util.ProgramDriver;

import com.kangfoo.study.hadoop1.mp.counter.TestCounter;
import com.kangfoo.study.hadoop1.mp.join.TestMapSideJoin;
import com.kangfoo.study.hadoop1.mp.join.TestReduceSideJoin;
import com.kangfoo.study.hadoop1.mp.sort.TestDefaultSort;
import com.kangfoo.study.hadoop1.mp.sort.TestTotalOrderPartitioner;
import com.kangfoo.study.hadoop1.mp.typeformat.TestCombiner;
import com.kangfoo.study.hadoop1.mp.typeformat.TestMapreduceMultipleInputs;
import com.kangfoo.study.hadoop1.mp.typeformat.TestMapreduceSequenceInputFormat;
import com.kangfoo.study.hadoop1.mp.typeformat.TestMapreduceTextInputFormat;
import com.kangfoo.study.hadoop1.mp.typeformat.TestPartitioner;
import com.kangfoo.study.hadoop1.mp.typeformat.TestRecordReader;

/**
 * A description of an example program based on its class and a human-readable
 * description.
 */
public class ExampleDriver {

	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {

			pgd.addClass("TestMapreduceInputFormat",
					TestMapreduceTextInputFormat.class,
					"wordcount测试mapreduce TextInputFormat输入格式");
			pgd.addClass("TestMapreduceSequenceInputFormat",
					TestMapreduceSequenceInputFormat.class,
					"wordcount测试mapreduce SequenceInputFormat输入格式");
			pgd.addClass("TestMapreduceMultipleInputs",
					TestMapreduceMultipleInputs.class,
					"wordcount测试mapreduce TestMapreduceMultipleInputs 多输入");
			pgd.addClass("TestCombiner", TestCombiner.class, "测试Combiner组件");
			pgd.addClass("TestPartitioner", TestPartitioner.class, "测试Partitioner组件");
			pgd.addClass("TestRecordReader", TestRecordReader.class, "测试RecordReader组件");
			pgd.addClass("TestCounter", TestCounter.class, "测试TestCounter组件");
			pgd.addClass("TestReduceSideJoin", TestReduceSideJoin.class, "测试TestReduceSideJoin组件");
			pgd.addClass("TestMapSideJoin", TestMapSideJoin.class, "测试TestMapSideJoin组件");
			pgd.addClass("TestDefaultSort", TestDefaultSort.class, "测试TestDefaultSort组件");
			pgd.addClass("TestTotalOrderPartitioner", TestTotalOrderPartitioner.class, "测试TestTotalOrderPartitioner组件");
			
			
					
			
			pgd.driver(argv);

			// Success
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}
}
