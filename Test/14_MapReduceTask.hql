//1、 Map并行度

---设置切片逻辑,合并小文件(默认开启)
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

--一个切片的最大值(适用于复杂逻辑)
set mapreduce.input.fileinputformat.split.maxsize=256000000;


//2、 Reduce并行度(可以自行指定，也可由hive根据mapjob自动估算)

--指定Reduce端并行度，默认值为-1，表示用户未指定
set mapreduce.job.reduces;

--Reduce端并行度最大值
set hive.exec.reducers.max;

--单个Reduce Task计算的数据量，用于估算Reduce并行度(map端输入文件大小/reducer计算大小) 结果并不是很准确
set hive.exec.reducers.bytes.per.reducer;

