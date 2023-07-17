
//1、 Map端输入文件合并

--可将多个小文件切片，合并为一个切片，进而由一个map任务处理(默认开启)
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;


//2、 Reduce端输出文件合并

/*
 合并Reduce端输出的小文件，是指将多个小文件合并成大文件。
 */

--开启合并map only(只有map任务)任务输出的小文件
set hive.merge.mapfiles=true;

--开启合并map reduce任务输出的小文件
set hive.merge.mapredfiles=true;

--合并后的文件大小
set hive.merge.size.per.task=256000000;

--触发小文件合并任务的阈值，若某计算任务输出的文件平均大小低于该值，则触发合并
set hive.merge.smallfiles.avgsize=16000000;



//3、 输出优化思路

/*
    （1）合理设置任务的Reduce端并行度
        若将上述计算任务的并行度设置为1，就能保证其输出结果只有一个文件。
    （2）启用Hive合并小文件优化
 */


