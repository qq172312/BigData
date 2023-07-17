//1、 Common join(分区创建多个maptask)

/*
    1.1 通过两表关联字段进行分区
    1.2 以关联字段个数，设置common join的数量
    1.3 相同的key在reduce端完成join操作
 */


desc extended order_detail;

//2、 map join(需要内存缓存小表)

/*
    2.1 适用于大表join小表的场景(小表数据通过本地任务处理)
    2.2 第一个Job会读取小表数据，将其制作为hash table，并上传至Hadoop分布式缓存（本质上是上传至HDFS）
    2.3 第二个Job会先从分布式缓存中读取小表数据，并缓存在Map Task的内存中，然后扫描大表数据，这样在map端即可完成关联操作
    2.4 一般拿map可用内存容量的1/2~2/3来缓存小表(小表文件大小)*10
 */

explain formatted select
    *
from order_detail t1
join province_info t3 on t1.province_id=t3.id
join product_info t2 on t1.product_id=t2.id;

explain formatted select
                      *
                  from order_detail t1
                           join product_info t2 on t1.product_id=t2.id
                           join province_info t3 on t1.province_id=t3.id;

//两个小表逻辑物理相交逻辑分离
explain formatted select
                      *
from order_detail t1
join province_info t2 on t1.province_id=t2.id
join province_info t3 on t1.province_id=t3.id;


set hive.auto.convert.join;
set hive.auto.convert.join.noconditionaltask;
set hive.auto.convert.join.noconditionaltask.size=1000000;

------优化方案一(保证任务稳定性)

//走有条件map join最后生成conditional task并将所有map join和commonjoin加入任务列表

//启用Map Join自动转换
set hive.auto.convert.join=true;

//不使用无条件转Map Join。
set hive.auto.convert.join.noconditionaltask=false;

//调整hive.mapjoin.smalltable.filesize参数，使其大于等于product_info。
set hive.mapjoin.smalltable.filesize=25285707;

------优化方案二(效率高，需要内存多)
//走无条件mapjoin生成最优计划，并将子任务合并

//启用Map Join自动转换
set hive.auto.convert.join=true;

//不使用无条件转Map Join。
set hive.auto.convert.join.noconditionaltask=true;

//调整hive.auto.convert.join.noconditionaltask.size参数
-- ，使其大于等于product_info和province_info之和。
set hive.auto.convert.join.noconditionaltask.size=25286076;

------优化方案三
//走无条件join但不合并小表，节省内存

//启用Map Join自动转换
set hive.auto.convert.join=true;

//不使用无条件转Map Join。
set hive.auto.convert.join.noconditionaltask=true;

//调整hive.auto.convert.join.noconditionaltask.size参数
-- ，使其小于product_info和province_info之和。
set hive.auto.convert.join.noconditionaltask.size=25285707;



//3、 bucket map join(需要内存缓存桶)

/*
    3.1 可以使用大表join大表的场景
    3.2 条件
        3.2.1 join的表全为分桶表
        3.2.2 一张表桶数量必须是另一张表桶的整数倍,保证分桶之间的对应关系
            (即小表的一块一定对应大表的整数倍桶关联,例如小表0，1 大表0123，则0肯定对应0，2桶，1肯定对应1，3桶,即对应关系)
        3.2.3 map端无需缓存整个小表，只需缓存单个的桶就行。
    3.3 桶对应关系: 根据指定分桶字段的hash值对应关系。
 */

show partitions order_detail;
show partitions payment_detail;

//数据准备
drop table if exists order_detail_bucketed;
create table order_detail_bucketed(
                                      id           string comment '订单id',
                                      user_id      string comment '用户id',
                                      product_id   string comment '商品id',
                                      province_id  string comment '省份id',
                                      create_time  string comment '下单时间',
                                      product_num  int comment '商品件数',
                                      total_amount decimal(16, 2) comment '下单金额'
)
    clustered by (id) into 16 buckets
    row format delimited fields terminated by '\t';

drop table if exists payment_detail_bucketed;
create table payment_detail_bucketed(
                                        id              string comment '支付id',
                                        order_detail_id string comment '订单id',
                                        user_id         string comment '用户id',
                                        payment_time    string comment '支付时间',
                                        total_amount    decimal(16, 2) comment '支付金额'
)
    clustered by (order_detail_id) into 8 buckets
    row format delimited fields terminated by '\t';

insert overwrite table order_detail_bucketed
select
    id,
    user_id,
    product_id,
    province_id,
    create_time,
    product_num,
    total_amount
from order_detail
where dt='2020-06-14';

insert overwrite table payment_detail_bucketed
select
    id,
    order_detail_id,
    user_id,
    payment_time,
    total_amount
from payment_detail
where dt='2020-06-14';


//优化思路: 所以当参与Join的表数据量均过大时，就可以考虑采用Bucket Map Join算法

--关闭cbo优化，cbo会导致hint信息被忽略，需将如下参数修改为false
set hive.cbo.enable=false;

--map join hint默认会被忽略(因为已经过时)，需将如下参数修改为false
set hive.ignore.mapjoin.hint=false;

--启用bucket map join优化功能,默认不启用，需将如下参数修改为true
set hive.optimize.bucketmapjoin = true;

explain extended select /*+ mapjoin(t2) */
    *
from order_detail_bucketed t1
join payment_detail_bucketed t2 on t1.id=t2.order_detail_id;








//4、 sort merge bucket map join(对内存(分桶大小)没有要求)

/*
        1、与上相比，多出了，要求桶内数据需要根据指定字段排序
        2、且分桶字段、排序字段、关联字段一致
        3、两个分桶间的join实现原理为sort merge join算法
        4、合并的时候不需要把表全部加载到内存，只需要按排序的字段滚动匹配(因为join字段有序，只需要判断上下值是否相同即可)
 */

//数据准备


--订单表
drop table if exists order_detail_sorted_bucketed;
create table order_detail_sorted_bucketed(
                                             id           string comment '订单id',
                                             user_id      string comment '用户id',
                                             product_id   string comment '商品id',
                                             province_id  string comment '省份id',
                                             create_time  string comment '下单时间',
                                             product_num  int comment '商品件数',
                                             total_amount decimal(16, 2) comment '下单金额'
)
    clustered by (id) sorted by(id) into 16 buckets
    row format delimited fields terminated by '\t';

--支付表
drop table if exists payment_detail_sorted_bucketed;
create table payment_detail_sorted_bucketed(
                                               id              string comment '支付id',
                                               order_detail_id string comment '订单明细id',
                                               user_id         string comment '用户id',
                                               payment_time    string comment '支付时间',
                                               total_amount    decimal(16, 2) comment '支付金额'
)
    clustered by (order_detail_id) sorted by(order_detail_id) into 8 buckets
    row format delimited fields terminated by '\t';



--订单表
insert overwrite table order_detail_sorted_bucketed
select
    id,
    user_id,
    product_id,
    province_id,
    create_time,
    product_num,
    total_amount
from order_detail
where dt='2020-06-14';

--分桶表
insert overwrite table payment_detail_sorted_bucketed
select
    id,
    order_detail_id,
    user_id,
    payment_time,
    total_amount
from payment_detail
where dt='2020-06-14';


//优化思路 : 推荐自动转换，不推荐hint提示(已过时)

-- --启动Sort Merge Bucket Map Join优化
set hive.optimize.bucketmapjoin.sortedmerge=true;
--使用自动转换SMB Join
set hive.auto.convert.sortmerge.join=true;



explain formatted select *
from order_detail_sorted_bucketed t1
join payment_detail_sorted_bucketed t2
on t1.id=t2.order_detail_id;