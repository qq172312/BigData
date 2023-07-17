set mapreduce.framework.name;

//1、分区表语法

//1.1 创建分区表 partitioned by (day string)
create table dept_partition(
    dept_no int,
    d_name string,
    loc string
)
    partitioned by (day string)
row format delimited fields terminated by '\t';

//1.2 读写数据
//load
load data local inpath '/opt/module/hive-3.1.3/datas/dept_220230518.log'
into table dept_partition
partition (day='20230518');

//insert
insert into dept_partition values (1,'得到',22,'20220101');

insert overwrite table dept_partition partition (day='20230521')
select
    dept_no,
    d_name,
    loc
from dept_partition
where day='20230520';

//1.3 查询
select * from dept_partition
where day='20230520';

//查询分区表数据时，可以将分区字段看作表的伪列，可像使用其他字段一样使用分区字段。
select dept_no,d_name,loc,day
from dept_partition
where day='20230518';

//1.4 操作
//1.4.1查询所有分区
show partitions dept_partition;

//1.4.2 创建分区
//添加单个、多个分区
alter table dept_partition
add partition (day='20230521');

//1.4.3 删除分区
alter table dept_partition
drop partition (day='20220101');

show partitions dept_partition;

//1.4.4 msck修复分区

//1、add partitions增加HDFS路径存在但元数据缺失的分区信息。可以一次修复多个分区
msck repair table dept_partition add partitions;

//2、drop partitions(分区)删除HDFS路径已经删除但元数据仍然存在的分区信息。
msck repair table dept_partition drop partitions ;

//3、sync partition，元数据与hdfs路径同步，相当于drop+add
msck repair table dept_partition sync partitions ;

//2、二级分区表(路径里面套路径)

create table dept_partition2(
                                deptno int,    -- 部门编号
                                dname string, -- 部门名称
                                loc string     -- 部门位置
)
    partitioned by (day string, hour string)
    row format delimited fields terminated by '\t';

//插入数据
load data local inpath '/opt/module/hive-3.1.3/datas/dept_20230520.log'
    into table dept_partition2
    partition(day='20220520', hour='12');

insert into dept_partition2 values ('30','工部','300','20220520','13');
insert into dept_partition2 values ('40','工部','400','20220520','11');

show partitions dept_partition2;

//查询
select *
from dept_partition2
where day='20220520' and hour='12';

show partitions dept_partition;
select *
from dept_partition
where day='20230518';


//3、动态分区
//写入分区由当前行最后一个字段值决定,就不需要通过用户指定分区了,直接可以通过insert插入

//总功能开关(默认开)
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition;

//一条insert语句可同时创建的最大分区个数默认1000
set hive.exec.max.dynamic.partitions=1000;

//严格非严格模式,严格:至少指定一个分区为静态分区
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition.mode;

//单个Mapper或者Reducer可同时创建的最大的分区个数，默认为100。
set hive.exec.max.dynamic.partitions.pernode=100;

//一条insert语句可以创建的最大的文件个数，默认100000。
set hive.exec.max.created.files=100000;
//当查询结果为空时且进行动态分区时，是否抛出异常，默认false。
set hive.error.on.empty.partition=false;

//操作
create table dept_partition_dynamic(
                                       id int,
                                       name string
)
    partitioned by (loc int)
    row format delimited fields terminated by '\t';

//设置为非严格模式
set hive.exec.dynamic.partition.mode = nonstrict;
//设置动态分区,以地点为分区
insert into table dept_partition_dynamic
partition (loc)
select
    dept_no,
    d_name,
    loc --该字段不需要手动指定,动态识别创建分区
        from dept;


show partitions dept_partition_dynamic;

//严格模式只能这样使用
insert into dept_partition_dynamic values (50,'销售部',2000);

//非严格模式才能这样使用
insert into dept_partition_dynamic partition (loc)
values (80,'测试部',2300);

create table depat(
    id int
    )
partitioned by(de_no string)
row format delimited fields terminated by '\t';
