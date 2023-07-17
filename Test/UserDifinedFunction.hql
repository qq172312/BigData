//自定义函数
use db_hive1;

//1、UDF(一进一出)
select WC("fdhfhs") num;
select my_len1("dsdwsdsdsdsd");

//创建永久函数

create function my_len1
    as "org.example.MyUDF"
    using jar "hdfs://hadoop201:8020/udf/hql_work-1.0-SNAPSHOT.jar";

create function my_len2
    as "org.example.MyUDF2"
    using jar "hdfs://hadoop201:8020/udf/hql_work-1.0-SNAPSHOT.jar";

//查看函数
show functions like "*my_len*";

select my_len2('asdf') my;
drop function my_len2;
drop function my_len1;