set mapreduce.framework.name=yarn;

//分桶表

/*
 首先为每行数据计算一个指定字段的数据的hash值，然后模以一个指定的分桶数，
 最后将取模运算结果相同的行，写入同一个文件中，这个文件就称为一个分桶（bucket）。
 分区针对的是数据的存储路径，分桶针对的是数据文件
 */

//1、基本语法
//1.1 建表
create table stu_buck(
    id int,
    name string
)
clustered by(id)
into 4 buckets
row format delimited fields terminated by '\t';

//1.2 数据装载
//数据分别以id的hash值分桶存入四个桶中
explain extended load data local inpath '/opt/module/hive-3.1.3/datas/student.txt'
into table stu_buck;

//2.1 分桶表排序(在clustered by(字段),sorted by(字段))
create table stu_buck_sort(
    id int,
    name string
)
clustered by(id) sorted by (id) --加排序函数(不要求和分桶字段完全一致,也可以加多个排序字段)
into 4 buckets
row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive-3.1.3/datas/student.txt'
into table stu_buck_sort;

select * from stu_buck_sort;

create table deptw(
    id int,
    loc int,

)
clustered by


