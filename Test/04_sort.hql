//排序
use db_hive1;

//1、order by 加limit map端会进行优化 即 limit 3 只会返回各map的前3条数据

select
    *
from emp
order by sal
limit 3;

//以下都很少使用
//2、sort by为每个reduce产生一个排序文件,内部排序

set mapreduce.job.reduces;

select

//3、distribute by 类似自定义分区partition//根据hash值

//4、cluster by 当distribute by和sort by字段相同时，可以使用cluster by方式。分区+排序