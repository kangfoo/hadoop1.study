/**
 * 
 */
/**
 * @date 2014年2月22日
 * @author kangfoo-mac
 * @version 1.0.0
 */
package com.kangfoo.study.hadoop1.streaming;

/**
 
 示例步骤：
 
 1. 准备文件（hadoop-streaming-1.2.1.jar 需要提前在 hadoop/src/contrib/streaming 文件夹编译）
-rw-rw-r--. 1 hadoop hadoop     48 2月  22 10:47 data
-rw-rw-r--. 1 hadoop hadoop 107399 2月  22 10:41 hadoop-streaming-1.2.1.jar
-rw-rw-r--. 1 hadoop hadoop    186 2月  22 10:45 mapper.pl
-rw-rw-r--. 1 hadoop hadoop    297 2月  22 10:55 reducer.pl

2. 执行
[hadoop@master11 streaming]$ ../bin/hadoop jar hadoop-streaming-1.2.1.jar -mapper mapper.pl -reducer reducer.pl -input /test/streaming -output /test/streamingout1 -file mapper.pl -file reducer.pl 
packageJobJar: [mapper.pl, reducer.pl, /home/hadoop/env/data/tmp/hadoop-unjar8664452965863153115/] [] /tmp/streamjob1696240084764464092.jar tmpDir=null
14/02/22 11:16:42 INFO util.NativeCodeLoader: Loaded the native-hadoop library
14/02/22 11:16:42 WARN snappy.LoadSnappy: Snappy native library not loaded
14/02/22 11:16:42 INFO mapred.FileInputFormat: Total input paths to process : 1
14/02/22 11:16:42 INFO streaming.StreamJob: getLocalDirs(): [/home/hadoop/env/mapreduce/local]
14/02/22 11:16:42 INFO streaming.StreamJob: Running job: job_201402211848_0021
14/02/22 11:16:42 INFO streaming.StreamJob: To kill this job, run:
14/02/22 11:16:42 INFO streaming.StreamJob: /home/hadoop/env/hadoop-1.2.1/libexec/../bin/hadoop job  -Dmapred.job.tracker=master11:9001 -kill job_201402211848_0021
14/02/22 11:16:42 INFO streaming.StreamJob: Tracking URL: http://master11:50030/jobdetails.jsp?jobid=job_201402211848_0021
14/02/22 11:16:43 INFO streaming.StreamJob:  map 0%  reduce 0%
14/02/22 11:16:48 INFO streaming.StreamJob:  map 100%  reduce 0%
14/02/22 11:16:57 INFO streaming.StreamJob:  map 100%  reduce 33%
14/02/22 11:16:58 INFO streaming.StreamJob:  map 100%  reduce 100%
14/02/22 11:17:01 INFO streaming.StreamJob: Job complete: job_201402211848_0021
14/02/22 11:17:01 INFO streaming.StreamJob: Output: /test/streamingout1

3. 查看结果
[hadoop@master11 streaming]$ ../bin/hadoop fs -lsr /test/streamingout1
-rw-r--r--   2 hadoop supergroup          0 2014-02-22 11:17 /test/streamingout1/_SUCCESS
drwxr-xr-x   - hadoop supergroup          0 2014-02-22 11:16 /test/streamingout1/_logs
drwxr-xr-x   - hadoop supergroup          0 2014-02-22 11:16 /test/streamingout1/_logs/history
-rw-r--r--   2 hadoop supergroup      16506 2014-02-22 11:16 /test/streamingout1/_logs/history/job_201402211848_0021_1393039002873_hadoop_streamjob1696240084764464092.jar
-rw-r--r--   2 hadoop supergroup      50380 2014-02-22 11:16 /test/streamingout1/_logs/history/job_201402211848_0021_conf.xml
-rw-r--r--   2 hadoop supergroup         40 2014-02-22 11:16 /test/streamingout1/part-00000

[hadoop@master11 streaming]$ ../bin/hadoop fs -cat /test/streamingout1/part-00000
hbase	1
hello	4
world	1
java	1
hadoop	1


*/