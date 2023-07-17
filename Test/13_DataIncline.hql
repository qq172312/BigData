//1、分组导致的数据倾斜

----------------1.1 Map-Side聚合优化:最佳状态下完全屏蔽数据倾斜

/*
    在map端先聚合各个分组内的数据，相当于combine
 */

--启用map-side聚合
set hive.map.aggr=true;

--关闭skew-groupby
set hive.groupby.skewindata;


--用于检测源表数据是否适合进行map-side聚合。检测的方法是：先对若干条数据进行map-side聚合，若聚合后的条数和聚合前的条数比值小于该值，则认为该表适合进行map-side聚合；否则，认为该表数据不适合进行map-side聚合，后续数据便不再进行map-side聚合。
set hive.map.aggr.hash.min.reduction=0.5;

--用于检测源表是否适合map-side聚合的条数。
set hive.groupby.mapaggr.checkinterval=100000;

--map-side聚合所用的hash table，占用map task堆内存的最大比例，若超出该值，则会对hash table进行一次flush。
set hive.map.aggr.hash.force.flush.memory.threshold=0.9;



-----------1.2 Skew-GroupBy优化

/*
    启动两个MR任务，第一个MR按照随机数分区，将数据分散发送到Reduce，完成部分聚合，
    第二个MR按照分组字段分区，完成最终聚合。
 */


--启用分组聚合数据倾斜优化

--启用skew-groupby
set hive.groupby.skewindata=true;
--关闭map-side聚合
set hive.map.aggr=false;

set mapreduce.framework.name=yarn;

//优化测试

explain formatted select
    province_id,
    count(*)
from order_detail
group by province_id;


//2、Join导致的数据倾斜

----------2.1 map join

/*
    即同上一节优化，map join,bucket mapjoin,SMB mj
 */


--启动Map Join自动转换
set hive.auto.convert.join=true;

--一个Common Join operator转为Map Join operator的判断条件,若该Common Join相关的表中,存在n-1张表的大小总和<=该值,则生成一个Map Join计划,此时可能存在多种n-1张表的组合均满足该条件,则hive会为每种满足条件的组合均生成一个Map Join计划,同时还会保留原有的Common Join计划作为后备(back up)计划,实际运行时,优先执行Map Join计划，若不能执行成功，则启动Common Join后备计划。
set hive.mapjoin.smalltable.filesize=250000;

--开启无条件转Map Join
set hive.auto.convert.join.noconditionaltask=true;

--无条件转Map Join时的小表之和阈值,若一个Common Join operator相关的表中，存在n-1张表的大小总和<=该值,此时hive便不会再为每种n-1张表的组合均生成Map Join计划,同时也不会保留Common Join作为后备计划。而是只生成一个最优的Map Join计划。
set hive.auto.convert.join.noconditionaltask.size=10000000;



------------2.2 skew join(性能不如第一个,只适用于内存特别小的情况)

/*
    为倾斜的大key单独启动一个map join任务进行计算，其余key进行正常的common join。
 */

--启用skew join优化
set hive.optimize.skewjoin=true;
--触发skew join的阈值，若某个key的行数超过该参数值，则触发
set hive.skewjoin.key=100000;


----------2.3 调整sql语句

//调整前
select
    *
from a join b
on a.id=b.id;


//调整后
select
    substr(t1.id,1,4) id,
    t1.name,
    t2.name
from (select
    concat(id,'_',cast(rand()*2 as int)) id,
    name
from a) t1
join (
    select
        concat(id,'_','0') id,
        name
    from b
    union all
    select
        concat(id,'_','1') id,
        name
    from b
) t2 on t1.id=t2.id;
