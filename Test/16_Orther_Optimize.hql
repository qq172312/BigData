//1、 CBO优化(Cost based Optimizer，即基于计算成本的优化)

/*
    1.1 Hive会计算同一SQL语句的不同执行计划的计算成本，并选出成本最低的执行计划。
    1.2 优化优先级为: 数据的行数、CPU、本地IO、HDFS IO、网络IO
    1.3 主要用于join优化,如join顺序(数据量小的表join在前面,大表先和小表join，结果数据量可能更小)
 */

--启用cbo优化
set hive.cbo.enable;

--为了测试效果更加直观，关闭map join自动转换
set hive.auto.convert.join=false;


//2、谓词下推(predicate pushdown)

/*
    2.1 尽量将过滤操作前移，以减少后续计算步骤的数据量。
    2.2 过滤操作放在join前
 */


--是否启动谓词下推（predicate pushdown）优化
set hive.optimize.ppd = true;

--为了测试效果更加直观，关闭cbo优化
set hive.cbo.enable=false;


//3、 矢量化查询

/*
    3.1 可以极大的提高一些典型查询场景（例如scans, filters, aggregates, and joins）下的CPU使用效率。
    3.2 执行计划出现 Execution mode: vectorized 表示启用了矢量化查询
    3.3 对整个数据集并行计算
 */

----开启矢量化查询
set hive.vectorized.execution.enabled=true;

//4、 Fetch抓取

/*
    Hive中对某些情况的查询可以不必使用MapReduce计算
 */


--是否在特定场景转换为fetch 任务
--设置为none表示不转换
--设置为minimal表示支持select *，分区字段过滤，Limit等
--设置为more表示支持select 任意字段,包括函数，过滤，和limit等
set hive.fetch.task.conversion=more;


//5、 本地模式

--开启自动转换为本地模式
set hive.exec.mode.local.auto=true;

--设置local MapReduce的最大输入数据量，当输入数据量小于这个值时采用local  MapReduce的方式，默认为134217728，即128M
set hive.exec.mode.local.auto.inputbytes.max=50000000;

--设置local MapReduce的最大输入文件个数，当输入文件个数小于这个值时采用local MapReduce的方式，默认为4
set hive.exec.mode.local.auto.input.files.max=10;


//6、 并行执行

/*
    6.1 sql语句转化为stage,每个stage对应一个MR job
    6.2 多个Stage可能并非完全互相依赖，也就是说有些Stage是可以并行执行的。
    6.3 此处提到的并行执行就是指这些Stage的并行执行(即一次提交多个mr)。
 */


--启用并行执行优化
set hive.exec.parallel=true;

--同一个sql允许最大并行度，默认为8
set hive.exec.parallel.thread.number=8;


//7、 严格模式

/*
    防止危险操作
 */

---------7.1 分区表不使用分区过滤

--设置为true时,对于分区表，除非where语句中含有分区字段过滤条件来限制范围，否则不允许执行。
set hive.strict.checks.no.partition.filter=true;


---------7.2 使用order by没有limit过滤

-- 7.2.1 设置为true时,对于使用了order by语句的查询，要求必须使用limit语句。
-- 7.2.2 因为order by为了执行排序过程会将所有的结果数据分发到同一个Reduce中进行处理
-- 7.2.3 开启了limit可以在数据进入到Reduce之前就减少一部分数据

set hive.strict.checks.orderby.no.limit=true;

---------7.3 笛卡尔积

--设置为true时,会限制笛卡尔积的查询。
set hive.strict.checks.cartesian.product=true;

explain formatted select
    *
from product_info
order by id
limit 100;
