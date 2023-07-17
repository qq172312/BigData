//1、窗口函数（跨行取值函数）

//1.1 基于行的窗口定义
//order by rows bettween 起点 and 中点
//必须声明order by字段，实际是mr顺序
select *,
    sum(cost) over(order by orderdate rows between 1 preceding and current row )
from business;

//1.2 基于值的矿口函数
//order by range between 开始值 and 结束值
//基于值的窗口函数中，不是必须order by

/*
 Shuffle 阶段在窗口函数计算中起到了关键的作用，
 它确保了数据按照分区和排序规则进行正确的划分和组织，
 以满足窗口函数的要求。在窗口函数计算之前，Shuffle 阶段将数据重新分区、排序和合并，
 为 Reduce 阶段中的窗口函数提供正确的数据集合。
 */
select *,
       sum(cost) over(partition by name order by orderdate range
           between unbounded preceding and current row ) num
from business;

//窗口内计算在reduce之后PTF操作,但是窗口内分区和排序是在shuffle阶段完成(且优先级没有窗口外高)
explain formatted select *,
       sum(cost) over (order by orderdate) num
from business
order by cost;

//窗口外排序在map最后，reduce之前进行(即shuffle阶段的排序),但窗口函数中的计算是在reduce阶段(shuffle之后)执行
explain formatted select *
    from business
order by orderdate;

//不加order by 会根据当前窗口划分字段值，计算
//窗口外和窗口内都是在map最后(shuffle阶段)完成的分组排序,既有窗口排序又有全局排序，则全局排序在最后执行
explain formatted select *,
       sum(cost) over (partition by name order by orderdate range between unbounded preceding and current row ) num
from business;

set mapreduce.framework.name=local;
//1.3缺省
//over中的partition by order by rows|range between and 均可省略不写
//partition by 不写表示不分区 order by 不写表示不排序
//如果指定order by则默认为分区起点到当前值的行(默认基于值进行累加)


//1.4 lag和lead(字段名，偏移量，默认值)获取上/下行数据
select *,
       lag(orderdate,1,'1970-01-01') over (partition by name order by orderdate) last,
       lead(orderdate,1,'9999-12-31') over (partition by name order by orderdate) next
from business;

//昨今明三天的消费
select *,
       lag(cost,1,0) over (partition by name order by orderdate)+cost
           +lead(cost,1,0) over (partition by name order by orderdate)
from business;

//1.5 first_value和 last_value(字段名,是否跳过null)获取最上/最下行值
select *,
       first_value(orderdate,false) over (partition by name order by orderdate) last,
       last_value(orderdate,false) over (partition by name order by orderdate) next
from business;

set mapreduce.framework.name=local;
//2、排名函数(不支持自定义窗口)
create table student1(
    stu_id int,
    course string,
    score int
);

insert into student1 values (1,'语文',99),
                            (2,'语文',98),
                            (3,'语文',95),
                            (4,'数学',100),
                            (5,'数学',100),
                            (6,'数学',99);

//2.1 排名rank(1,1,3)即重复值算行号,dense_rank(1,1,2)重复值不算行号,row_number(1,2,3)行号
select *,
       rank() over (partition by course order by score desc ) rk,
       dense_rank() over (partition by course order by score desc ) d_rk,
       row_number() over (partition by course order by score desc ) r_num
from student1 ;





