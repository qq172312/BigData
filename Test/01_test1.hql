show functions;

show databases;

//四舍五入
select round(3.5);

//向上取整
select ceil(3.1);

//向下取整
select floor(4.8);

//截取字符串
select substr('1234', 2, 2) - substr('4321', 2, 2);
select substring('1234',2,4);

//获取倒数第三个以后的所有字符
select substring('123456', -3);
select substring('123456', 4);

//替换
select replace('atguigu', 'a', 'A');
select replace('123456','3','张');

//正则替换,将数字替换为num(d为匹配单个数字,d+为匹配所有数字)
select regexp_replace('100-200', '(\\d+)', 'num');
select regexp_replace('21-1-1-1-','(\\d)','字符');

//正则匹配(判断是否匹配上)
select 'atguigu' regexp 'at+' one;
select 'sdsdsd' regexp 'sd+' two;

//将字符串重复3编123123123
select repeat('123', 3);

//字符串切割(第二个参数不只是切割字符,而是一个正则表达式)
select split('a-t-g-u-i-g-u', '-');
select split('192.168.0.1', '\\.');
select explode(split('one-two-three-four-five','-'));

//替换null值为1(A值为不为nll则返回A值,否则返回B值)
select nvl(null, 'b');

//拼接字符串
select concat('beijing', '-', '欢迎你', '-', '来往');

//指定分隔符拼接字符串(无法处理int型数组)
select concat_ws('-', '北京', '欢迎你', '来往');
select concat_ws('-', `array`('北京', '上海', '南京'));

//解析json字符串
select get_json_object('[{"name":"大海海","sex":"男","age":"23"},{"name":"小宋宋","sex":"男","age":"47"}]',
                       '$.[1].name');

select get_json_object('[{"name":"大海海","sex":"男","age":"23"},{"name":"小宋宋","sex":"男","age":"47"}]', '$.[1]');


//日期函数
//返回时间戳
select unix_timestamp('2022/08/08 08-08-08', 'yyyy/MM/dd HH-mm-ss');

//转化时间戳
select from_unixtime(1684511368);

select `current_date`();
select `current_timestamp`();
select current_groups();

select month('2023-5-14 08:08:08');

select day(`current_date`());

select hour(current_timestamp);

select datediff(`current_date`(), '2023-3-15');
select datediff(`current_date`(),'2023-6-11');

select date_add(`current_date`(), 20);

select date_sub(`current_date`(), 20);

select date_format(`current_date`(), 'yyyy年-MM月-dd日');

//条件判断函数case when
select case
           when 1 = 2 then 'tom'
           when 2 = 2 then 'marry'
           else 'tim'
           end;

select case 101
           when 50 then 'tom'
           when 100 then 'marry'
           else 'tim' end;

use db_hive1;

select current_database();

create table emp_sex
(
    name    string,
    dept_id string,
    sex     string
)
    row format delimited fields terminated by '\t';
load data local inpath '/opt/module/hive-3.1.3/datas/emp_sex.txt'
    into table emp_sex;

//统计各部门男女出现次数
select dept_id,
       sum(case sex when '男' then 1 else 0 end) male,
       sum(case sex when '男' then 1 else 0 end) female
from emp_sex
group by dept_id;

//条件判断
select if(3 > 5, '是', '否');

//集合函数
//集合中元素个数
create table test
(
    friends map<string,int>
)
    row format delimited map keys terminated by ':';

insert into test
values (`map`('张三', 1));
insert into test
values (`map`('李四', 2));

//统计集合中元素个数,不能统计结构体内元素个数
select size(friends)
from test;

insert into test
values (`map`('one', 1, 'two', 2, 'three', 3));

//返回map中的key
select map_keys(friends)
from test;
//返回map中的values
select map_values(friends)
from test;

//返回array中的元素
select `array`('1', '2', '3', '4', '5');
//判断数组中是否包含'2'
select array_contains(`array`('1', '2', '3', '4', '5'), '6');
//排序
select sort_array(`array`('1', '4', '3', '2', '5'));
//根据参数排序
select sort_array_by();

//struct声明属性
select struct('name', 'age', 'weight');
//创建键值
select named_struct('name', 'xiaosong', 'age', 18, 'weight', 80);

select struct('one','two','three','four');
select named_struct('one','two','three','four');

//案例1
create table employee
(
    name     string,         --姓名
    sex      string,         --性别
    birthday string,         --出生年月
    hiredate string,         --入职日期
    job      string,         --岗位
    salary   double,         --薪资
    bonus    double,         --奖金
    friends  array<string>,  --朋友
    children map<string,int> --孩子
);

insert into employee
values ('张无忌', '男', '1980/02/12', '2022/08/09', '销售', 3000, 12000, array('阿朱', '小昭'),
        map('张小无', 8, '张小忌', 9)),
       ('赵敏', '女', '1982/05/18', '2022/09/10', '行政', 9000, 2000, array('阿三', '阿四'), map('赵小敏', 8)),
       ('宋青书', '男', '1981/03/15', '2022/04/09', '研发', 18000, 1000, array('王五', '赵六'),
        map('宋小青', 7, '宋小书', 5)),
       ('周芷若', '女', '1981/03/17', '2022/04/10', '研发', 18000, 1000, array('王五', '赵六'),
        map('宋小青', 7, '宋小书', 5)),
       ('郭靖', '男', '1985/03/11', '2022/07/19', '销售', 2000, 13000, array('南帝', '北丐'),
        map('郭芙', 5, '郭襄', 4)),
       ('黄蓉', '女', '1982/12/13', '2022/06/11', '行政', 12000, null, array('东邪', '西毒'),
        map('郭芙', 5, '郭襄', 4)),
       ('杨过', '男', '1988/01/30', '2022/08/13', '前台', 5000, null, array('郭靖', '黄蓉'), map('杨小过', 2)),
       ('小龙女', '女', '1985/02/12', '2022/09/24', '前台', 6000, null, array('张三', '李四'), map('杨小过', 2));

select month(replace(hiredate, '/', '-')) `month`, count(*) cnt
from employee
group by month(replace(hiredate, '/', '-'));

//查询每个人的年龄（年 + 月)
select
    t2.name,
    concat(`if`(t2.month>=0,t2.year,t2.year-1),'年',
        `if`(t2.month>=0,t2.month,t2.month+12),'月') age
from (select name,
             year(`current_date`()) - year(t1.birthday)   `year`,
             month(`current_date`()) - month(t1.birthday) `month`
      from (select name,
                   replace(birthday, '/', '-') `birthday`
            from employee) t1) t2;

//按照薪资，资金的和进行倒序排序
select name,salary+nvl(bonus,0) sal
from employee
sort by sal desc;

//查询朋友个数
select name,size(friends) cnt
from employee;

//查询每个人孩子的姓名
select name,map_keys(children) ch_name
from employee;

//查询岗位男女个数
select job,
       sum(case sex when '男' then 1 else 0 end ) male,
       sum(case sex when '女' then 1 else 0 end ) female
from employee
group by job;

//高级聚合函数
//收集并形成集合
select
    sex,
    collect_list(job)
from employee
group by sex;

select
    sex,
    collect_list(name)
from employee
group by sex
order by sex desc ;

use db_hive1;

select
    sex,
    collect_set(name)
from employee
group by sex
order by sex;

//每个月的入职人数以及姓名
select month(replace(hiredate,'/','-')) `month`,collect_list(name),count(*) cnt
from employee
group by month(replace(hiredate,'/','-'));

select replace(hiredate,'/','-') a
from employee;

//炸裂函数UDTF
//数据准备
create table movie_info(
    movie string,
    category string
)
row format delimited fields terminated by '\t';

//装载数据
insert overwrite table movie_info
values("《疑犯追踪》", "悬疑,动作,科幻,剧情"),
      ("《Lie to me》", "悬疑,警匪,动作,心理,剧情"),
      ("《战狼2》", "战争,动作,灾难");

//统计各分类的电影数量
select
    t2.cate,
    count(*)
from (select movie,
        cate
 from (select movie,
              split(category, ',') cates
       from movie_info) t1 lateral view explode(cates) tmp as cate
 ) t2
group by t2.cate;

select cate
from (select split(category,',') cates
from movie_info) t1 lateral view explode(cates) tmp as cate;

select explode(t1.cates) cate
from (select split(category,',') cates
      from movie_info) t1;
//将数组按行输出
select explode(t1.arr)
from (select
          split('one-two-three','-') arr) t1;

//lateral view
select movie,
       hobby
from movie_info
lateral view explode(split(category,',')) tmp as hobby;

//统计各分类电影数量
select cates,count(*) num
from movie_info
lateral view explode(split(category,',')) tmp as cates
group by cates;

select
    cates,
    movie
from movie_info
lateral view explode(split(category,',')) tmp as cates
sort by cates;



//窗口函数(输入多行返回一行)

//跨行取值

show tables ;
//创建表
create table business(
                         name string,
                         orderdate string,
                         cost int

)
row format delimited fields terminated by ',';
load data local inpath "/opt/module/hive-3.1.3/datas/business.txt" into table business;

select name,count(name) over() num
from business
where month(orderdate)=4
group by name;

//以name分区，再以月份分区
select name,orderdate,cost,sum(cost) over(partition by name,month(orderdate))
from business;

//按日期累加
select name,orderdate,cost,sum(cost) over(partition by name,month(orderdate) order by orderdate)
from business;

//窗口范围测试
select
    name,
    orderdate,
    cost,
    sum(cost) over(partition by month(orderdate)) total_cost
from business;


