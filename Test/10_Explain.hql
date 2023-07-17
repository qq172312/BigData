//explain 执行计划

set mapreduce.framework.name;

//map
set mapreduce.map.memory.mb;
set mapreduce.map.cpu.vcores=1;
//reduce
set mapreduce.reduce.memory.mb;
set mapreduce.reduce.cpu.vcores;

//一个operator对应一个map或reduce端的一个单一的逻辑操作,例如tablescan operate,select operate

explain formatted
select count(*)
from order_detail
where dt <= '2020-06-14'
group by user_id;

//SQL优化(Map端)

--1、分组聚合

explain formatted select
    product_id,
    count(*)
from order_detail
group by product_id;

--优化前 46s
set hive.map.aggr;
set hive.map.aggr=true;
set hive.map.aggr.hash.min.reduction=1;
set mapreduce.job.reduces;

set mapreduce.framework.name=yarn;

--优化后

--用于检测源表是否适合map-side聚合的条数。
set hive.map.aggr=true;

--用于检测源表数据是否适合进行map-side聚合。检测的方法是：先对若干条数据进行map-side聚合，若聚合后的条数和聚合前的条数比值小于该值，则认为该表适合进行map-side聚合；否则，认为该表数据不适合进行map-side聚合，后续数据便不再进行map-side聚合。
set hive.map.aggr.hash.min.reduction=0.5;

--用于检测源表是否适合map-side聚合的条数。
set hive.groupby.mapaggr.checkinterval=10000;

--map-side聚合所用的hash table，占用map task堆内存的最大比例，若超出该值，则会对hash table进行一次flush。
set hive.map.aggr.hash.min.reduction=0.9;
