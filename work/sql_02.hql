//中级题目

//2.1 [课堂讲解]查询累积销量排名第二的商品

//查询订单明细表（order_detail）中销量（下单件数）排名第二的商品id，如果不存在返回null，如果存在多个排名第二的商品则需要全部返回。
//sku_id
//11
select sku_id
from (select sku_id,
             dense_rank() over (order by count(sku_num) over (partition by sku_id)) nol,
             row_number() over (partition by sku_id)                                rw
      from order_detail) t1
where t1.nol = 2
  and rw = 1;

//2.2 [课堂讲解]查询至少连续三天下单的用户

//查询订单信息表(order_info)中最少连续3天下单的用户id，期望结果如下：

//user_id
//101

select user_id
from (select distinct create_date,
                      user_id,
                      `if`(lag(create_date, 1, 0) over (partition by user_id) = date_sub(create_date, 1), 1, 0)  up,
                      `if`(lead(create_date, 1, 0) over (partition by user_id) = date_add(create_date, 1), 1, 0) down
      from order_info
      order by user_id) t1
where t1.up = 1
  and t1.down = 1;

//优化后的
select t2.user_id
from (select user_id
      from (select user_id,
                   `if`(lag(create_date, 1, 0) over (partition by user_id) = date_sub(create_date, 1), 1, 0)  up,
                   `if`(lead(create_date, 1, 0) over (partition by user_id) = date_add(create_date, 1), 1, 0) down
            from order_info
            group by user_id, create_date) t1
      where t1.down = 1
        and t1.up = 1) t2
group by t2.user_id;

//参考
select user_id
from (select user_id
           , create_date
           , date_sub(create_date, row_number() over (partition by user_id order by create_date)) flag
      from (select user_id
                 , create_date
            from order_info
            group by user_id, create_date) t1 -- 同一天可能多个用户下单，进行去重
     ) t2 -- 判断一串日期是否连续：若连续，用这个日期减去它的排名，会得到一个相同的结果
group by user_id, flag
having count(flag) >= 3;
-- 连续下单大于等于三天

//2.3 [课堂讲解]查询各品类销售商品的种类数及销量最高的商品

//从订单明细表(order_detail)统计各品类销售出的商品种类数及累积销量最好的商品，期望结果如下

// 10s 完成 优化，里边小外边大,尽可能减少内层行数
select *
from (select t3.category_id                                                            `分类id`,
             t4.category_name                                                          `分类名称`,
             if(max(num) over (partition by t3.category_id) = t3.num, t3.sku_id, null) `商品id`,
             t3.name                                                                   `商品名称`,
             max(num) over (partition by t3.category_id)                               `销量最好商品销量`,
             count(t3.sku_id) over (partition by t3.category_id)                       `商品种类数`
      from category_info t4
               join
           (select t1.sku_id,
                   collect_set(t2.name)[0]        name,
                   sum(t1.sku_num)                num,
                   collect_set(t2.category_id)[0] category_id
            from order_detail t1
                     join sku_info t2
                          on t1.sku_id = t2.sku_id
            group by t1.sku_id) t3
           on t4.category_id = t3.category_id) t4
where t4.`商品id` is not null;
//having max(num)=t3.num;

//12s
select t5.`分类id`,
       t5.`分类名`,
       t5.`商品id`,
       t5.`商品名`,
       t5.`销量`,
       t5.`商品种类数`
from (select t3.category_id                                                    `分类id`,
             t4.category_name                                                  `分类名`,
             t3.sku_id                                                         `商品id`,
             t3.name                                                           `商品名`,
             t3.num                                                            `销量`,
             row_number() over (partition by t3.category_id order by num desc) rk,
             count(t3.sku_id) over (partition by t3.category_id)               `商品种类数`
      from category_info t4
               join
           (select t1.sku_id,
                   collect_set(t2.name)[0]        name,
                   sum(t1.sku_num)                num,
                   collect_set(t2.category_id)[0] category_id
            from order_detail t1
                     join sku_info t2
                          on t1.sku_id = t2.sku_id
            group by t1.sku_id) t3
           on t4.category_id = t3.category_id) t5
where rk = 1;
//having max(num)=t3.num;

//5s
select t1.sku_id,
       sum(t1.sku_num),
       t2.category_id
from order_detail t1
         join sku_info t2
              on t1.sku_id = t2.sku_id
group by t1.sku_id;

//6s
select distinct t1.sku_id,
                sum(t1.sku_num) over (partition by t1.sku_id),
                t2.category_id
from order_detail t1
         join sku_info t2
              on t1.sku_id = t2.sku_id;

//参考 7s
select category_id,
       category_name,
       sku_id,
       name,
       order_num,
       sku_cnt
from (select od.sku_id,
             sku.name,
             sku.category_id,
             cate.category_name,
             order_num,
             rank() over (partition by sku.category_id order by order_num desc) rk,
             count(distinct od.sku_id) over (partition by sku.category_id)      sku_cnt
      from (select sku_id,
                   sum(sku_num) order_num
            from order_detail
            group by sku_id) od
               left join
           sku_info sku
           on od.sku_id = sku.sku_id
               left join
           category_info cate
           on sku.category_id = cate.category_id) t1
where rk = 1;


//2.4 [课堂讲解]查询用户的累计消费金额及VIP等级

//从订单信息表(order_info)中统计每个用户截止其每个下单日期的累积消费金额，以及每个用户在其每个下单日期的VIP等级。
/*
 用户vip等级根据累积消费金额计算，计算规则如下：
设累积消费总额为X，
若0=<X<10000,则vip等级为普通会员
若10000<=X<30000,则vip等级为青铜会员
若30000<=X<50000,则vip等级为白银会员
若50000<=X<80000,则vip为黄金会员
若80000<=X<100000,则vip等级为白金会员
若X>=100000,则vip等级为钻石会员

 id     下单日期    累计消费金额  下单日的vip等级
 */

//2.5s
select t1.user_id,
       t1.create_date,
       t1.sum_so_far,
       t1.vip_level
from (select user_id,
             create_date,
             sum(total_amount) over (partition by user_id order by create_date) sum_so_far,
             case
                 when sum(total_amount) over (partition by user_id order by create_date) >= 100000 then '钻石会员'
                 when sum(total_amount) over (partition by user_id order by create_date) >= 80000 then '白金会员'
                 when sum(total_amount) over (partition by user_id order by create_date) >= 50000 then '黄金会员'
                 when sum(total_amount) over (partition by user_id order by create_date) >= 30000 then '白银会员'
                 when sum(total_amount) over (partition by user_id order by create_date) >= 10000 then '青铜会员'
                 else '普通会员' end                                            vip_level,
             row_number() over (partition by user_id,create_date)               rw
      from order_info) t1
where t1.rw = 1;

//1.5s
select user_id,
       create_date,
       sum_t,
       case
           when sum_t >= 100000 then '钻石会员'
           when sum_t >= 80000 then '白金会员'
           when sum_t >= 50000 then '黄金会员'
           when sum_t >= 30000 then '白银会员'
           when sum_t >= 10000 then '青铜会员'
           else '普通会员' end vip_level
from (select user_id,
             create_date,
             sum(sum_total_amount) over (partition by user_id order by create_date) sum_t
      from (select user_id,
                   create_date,
                   sum(total_amount) sum_total_amount
            from order_info
            group by user_id, create_date) t1) t2;

//参考2 3s
SELECT user_id,
       create_date,
       sum_so_far,
       CASE
           WHEN sum_so_far >= 100000 THEN '钻石会员'
           WHEN sum_so_far >= 80000 THEN '白金会员'
           WHEN sum_so_far >= 50000 THEN '黄金会员'
           WHEN sum_so_far >= 30000 THEN '白银会员'
           WHEN sum_so_far >= 10000 THEN '青铜会员'
           ELSE '普通会员'
           END AS vip_level
FROM (SELECT user_id,
             create_date,
             sum(total_amount) over (partition by user_id order by create_date)           sum_so_far,
             ROW_NUMBER() OVER (PARTITION BY user_id,create_date ORDER BY create_date) AS row_num
      FROM order_info) AS subquery
WHERE row_num = 1
ORDER BY create_date;

//2.5 [课堂讲解]查询首次下单后第二天连续下单的用户比率

/*
 从订单信息表(order_info)中查询首次下单后第二天仍然下单的用户占所有下单用户的比例，
 结果保留一位小数，使用百分数显示，期望结果如下：
 percentage
    70.0%
 */
select *
from (select user_id,
             create_date,
             dense_rank() over (partition by user_id order by create_date)                    no,
             first_value(create_date, false) over (partition by user_id order by create_date) one
      from order_info) t1
where t1.no = 1
   or t1.no = 2;
//group by user_id;

//实现4s
select concat(round(count(*) / collect_set(num1)[0] * 100, 1), '%')
from (select user_id,
             collect_set(num)[0] num1
      from (select user_id,
                   dense_rank() over (partition by user_id order by create_date)                              no,
                   sum(`if`(row_number() over (partition by user_id order by create_date) = 1, 1, 0)) over () num
            from order_info) t1
      where t1.no = 2
      group by user_id) t2;

//参考 3s 尽量使用group by去重
select concat(round(sum(if(datediff(buy_date_second, buy_date_first) = 1, 1, 0)) / count(*) * 100, 1), '%') percentage
from (select user_id,
             min(create_date) buy_date_first,
             max(create_date) buy_date_second
      from (select user_id,
                   create_date,
                   rank() over (partition by user_id order by create_date) rk
            from (select user_id,
                         create_date
                  from order_info
                  group by user_id, create_date) t1) t2
      where rk <= 2
      group by user_id) t3;


//改进

select concat(round(count(num) / count(*) * 100, 1), '%') percentage
from (select user_id,
             sum(`if`(rk = 2, 1, null)) num
      from (select user_id,
                   create_date,
                   rank() over (partition by user_id order by create_date)                          rk,
                   first_value(create_date, false) over (partition by user_id order by create_date) first
            from (select user_id,
                         create_date
                  from order_info
                  group by user_id, create_date) t1) t2
      where rk <= 2
      group by user_id) t3;



//2.14 向用户推荐朋友收藏的商品

/*
现需要请向所有用户推荐其朋友收藏但是用户自己未收藏的商品，
请从好友关系表（friendship_info）和收藏表（favor_info）中查询出应向哪位用户推荐哪些商品。期望结果如下：

user_id（用户id）	sku_id（应向该用户推荐的商品id）
101	2
101	4
101	7
101	9
101	8
101	11
101	1
 */

//实现 20s
select t5.user_id,
       collect_set(sku_id) sku_id
from (select nvl(t4.user_id, t3.user1_id) user_id,
             t3.sku_id
      from favor_info t4
               right join(select user1_id,
                                 sku_id,
                                 row_number() over (partition by user1_id,sku_id) rn
                          from friendship_info t1
                                   join favor_info t2
                                        on t1.user2_id = t2.user_id
                          order by user1_id) t3
                         on t4.user_id = t3.user1_id
                             and t4.sku_id = t3.sku_id
      where rn = 1
        and t4.sku_id is null) t5
group by t5.user_id;



select user1_id,
       sku_id,
       row_number() over (partition by sku_id) rn
from friendship_info t1
         join favor_info t2
              on t1.user2_id = t2.user_id
where user1_id = 101
order by user1_id;


select t4.user_id,
       collect_set(t4.sku_id) one,
       collect_set(al)[0]     two
from favor_info t4
         join (select user1_id,
                      collect_set(sku_id) al
               from friendship_info t1
                        join favor_info t2
                             on t1.user2_id = t2.user_id
               where user1_id = 101
               group by user1_id
               order by user1_id) t3
              on t3.user1_id = t4.user_id
group by t4.user_id;

//2.40 同时在线最多的人数

/*
 根据用户登录明细表（user_login_detail），求出平台同时在线最多的人数。
结果如下：
cn
 7
 */

select max(sum_1_time) cnt
from (select sum(flag) over (sort by t1.1_time) sum_1_time
      from (select login_ts 1_time,
                   1        flag
            from user_login_detail
            union
            select logout_ts 1_time,
                   -1        flag
            from user_login_detail) t1) t2;

select sum(flag) over (sort by t1.l_time) num
from (select user_id,
             login_ts l_time,
             1        flag
      from user_login_detail
      union
      select user_id,
             logout_ts l_time,
             -1        flag
      from user_login_detail) t1;


//2.6 每个商品销售首年的年份、销售数量和销售金额

//1.4s
select t1.sku_id,
       t1.year,
       t1.order_num,
       t1.order_amount
from (select sku_id,
             year(create_date)                                                  year,
             sum(sku_num)                                                       order_num,
             sum(price)                                                         order_amount,
             row_number() over (partition by sku_id order by year(create_date)) rw
      from order_detail
      group by sku_id, year(create_date)
      order by sku_id) t1
where t1.rw = 1;

//参考2.5s
select sku_id,
       year(create_date),
       sum(sku_num),
       sum(price * sku_num)
from (select order_id,
             sku_id,
             price,
             sku_num,
             create_date,
             rank() over (partition by sku_id order by year(create_date)) rk
      from order_detail) t1
where rk = 1
group by sku_id, year(create_date);


//2.7 筛选去年总销量小于100的商品

select date_sub('2022-01-10', 30);
//7s 效率低,join尽量放在子循环里
explain formatted
select t2.sku_id,
       t1.name,
       t2.num order_num
from (select sku_id,
             sum(sku_num) num
      from order_detail
      where create_date between date_sub('2022-01-10', 365) and date_sub('2022-01-10', 30)
      group by sku_id) t2
         join sku_info t1
              on t1.sku_id = t2.sku_id;

//5s
explain formatted
select t1.sku_id,
       sum(t1.sku_num)         num,
       collect_set(t2.name)[0] name
from order_detail t1
         join sku_info t2
              on t1.sku_id = t2.sku_id
where create_date between date_sub('2022-01-10', 365) and date_sub('2022-01-10', 30)
group by t1.sku_id
having num < 100;

//参考 11s
select t1.sku_id,
       name,
       order_num
from (select sku_id,
             sum(sku_num) order_num
      from order_detail
      where year(create_date) = '2021'
        and sku_id in (select sku_id
                       from sku_info
                       where datediff('2022-01-10', from_date) > 30)
      group by sku_id
      having sum(sku_num) < 100) t1
         left join
     sku_info t2
     on t1.sku_id = t2.sku_id;


//2.8 查询每日新用户数

//2.3s
select t1.l_date login_date_first,
       count(*)  user_count
from (select user_id,
             substr(login_ts, 1, 10)                  l_date,
             row_number() over (partition by user_id) rw
      from user_login_detail
      group by user_id, substr(login_ts, 1, 10)) t1
where rw = 1
group by t1.l_date;


//参考 2.4s
select login_date_first,
       count(*) user_count
from (select user_id,
             min(date_format(login_ts, 'yyyy-MM-dd')) login_date_first
      from user_login_detail
      group by user_id) t1
group by login_date_first;


//2.9 统计每个商品的销量最高的日期

//2.5s 尽量不要在窗口内使用运算函数 如sum等
select t1.sku_id,
       t1.create_date,
       t1.num
from (select sku_id,
             create_date,
             sum(sku_num)                                                        num,
             row_number() over (partition by sku_id order by sum(sku_num) desc ) rw
      from order_detail
      group by sku_id, create_date) t1
where rw = 1;

//参考 2.4s
select sku_id,
       create_date,
       sum_num
from (select sku_id,
             create_date,
             sum_num,
             row_number() over (partition by sku_id order by sum_num desc,create_date asc) rn
      from (select sku_id,
                   create_date,
                   sum(sku_num) sum_num
            from order_detail
            group by sku_id, create_date) t1) t2
where rn = 1;


//2.10 查询销售件数高于品类平均数的商品

//7s 尽量避免使用集合(宁愿使用join (select)
select t4.sku_id,
       t4.num,
       t4.name
from (select t3.sku_id,
             t3.num,
             t3.name,
             avg(t3.num) over (partition by t3.id) avg_num
      from (select t1.sku_id,
                   sum(t1.sku_num)                num,
                   collect_set(t2.name)[0]        name,
                   collect_set(t2.category_id)[0] id
            from order_detail t1
                     join sku_info t2
                          on t1.sku_id = t2.sku_id
            group by t1.sku_id) t3) t4
where num > avg_num;


//参考 6s

select sku_id,
       name,
       sum_num,
       cate_avg_num
from (select od.sku_id,
             category_id,
             name,
             sum_num,
             avg(sum_num) over (partition by category_id) cate_avg_num
      from (select sku_id,
                   sum(sku_num) sum_num
            from order_detail
            group by sku_id) od
               left join
           (select sku_id,
                   name,
                   category_id
            from sku_info) sku
           on od.sku_id = sku.sku_id) t1
where sum_num > cate_avg_num;


//2.11 用户注册、登录、下单综合统计

//7s
select t1.user_id,
       substr(t1.login_ts, 1, 10) register_date,
       t1.total_login_count,
       t1.login_count_2021,
       t2.total_amount_2021
from (select user_id,
             login_ts,
             row_number() over (partition by user_id order by login_ts)            rw,
             count(*) over (partition by user_id)                                  total_login_count,
             count(if(year(login_ts) = 2021, 1, null)) over (partition by user_id) login_count_2021
      from user_login_detail) t1
         join (select user_id,
                      sum(total_amount) total_amount_2021
               from order_info
               where year(create_date) = 2021
               group by user_id) t2 on t2.user_id = t1.user_id
where rw = 1;

//优化后
select t1.user_id,
       register_date,
       total_login_count,
       login_count_2021,
       order_count_2021,
       total_amount_2021
from (select user_id,
             min(substr(login_ts, 1, 10))              register_date,
             count(1)                                  total_login_count,
             count(if(year(login_ts) = 2021, 1, null)) login_count_2021
      from user_login_detail
      group by user_id) t1
         join (select user_id,
                      count(distinct (order_id)) order_count_2021,
                      sum(total_amount)          total_amount_2021
               from order_info
               where year(create_date) = 2021
               group by user_id) t2 on t2.user_id = t1.user_id;


//参考

select login.user_id,
       register_date,
       total_login_count,
       login_count_2021,
       order_count_2021,
       order_amount_2021
from (select user_id,
             min(date_format(login_ts, 'yyyy-MM-dd'))    register_date,
             count(1)                                    total_login_count,
             count(if(year(login_ts) = '2021', 1, null)) login_count_2021
      from user_login_detail
      group by user_id) login
         join
     (select user_id,
             count(distinct (order_id)) order_count_2021,
             sum(total_amount)          order_amount_2021
      from order_info
      where year(create_date) = '2021'
      group by user_id) oi
     on login.user_id = oi.user_id;


//2.12 查询指定日期的全部商品价格

//1.2s
select t1.sku_id,
       t1.new_price price
from (select sku_id,
             new_price,
             row_number() over (partition by sku_id order by change_date desc ) rw
      from sku_price_modify_detail
      where change_date <= '2021-10-01') t1
where t1.rw = 1;

//2.13 即时订单比例

select round(count(`if`(t1.order_date = t1.custom_date, 1, null)) / count(1), 2) percentage
from (select user_id,
             order_date,
             custom_date,
             row_number() over (partition by user_id order by order_date) rw
      from delivery_info) t1
where rw = 1;


//2.15 查询所有用户的连续登录两天及以上的日期区间

select t3.user_id,
       t3.start_date,
       t3.end_date
from (select t2.user_id,
             first_value(t2.login_date, false)
                         over (partition by user_id order by login_date range between unbounded preceding and unbounded following) start_date,
             last_value(t2.login_date, false)
                        over (partition by user_id order by login_date range between unbounded preceding and unbounded following)  end_date,
             row_number() over (partition by user_id)                                                                              rw
      from (select *,
                   if(date_sub(login_date, 1) = lag(login_date, 1, '9999-09-09') over (partition by user_id), 1,
                      0) last,
                   if(date_add(login_date, 1) = lead(login_date, 1, '9999-09-09') over (partition by user_id), 1,
                      0) next
            from (select user_id,
                         substr(login_ts, 1, 10) login_date
                  from user_login_detail
                  group by user_id, substr(login_ts, 1, 10)) t1) t2
      where (last + next) >= 1) t3
where rw = 1;


//参考 2.7s

select user_id,
       min(login_date) start_date,
       max(login_date) end_date
from (select user_id,
             login_date,
             date_sub(login_date, rn) flag
      from (select user_id,
                   login_date,
                   row_number() over (partition by user_id order by login_date) rn
            from (select user_id,
                         date_format(login_ts, 'yyyy-MM-dd') login_date
                  from user_login_detail
                  group by user_id, date_format(login_ts, 'yyyy-MM-dd')) t1) t2) t3
group by user_id, flag
having count(*) >= 2;


//2.16 男性和女性每日的购物总金额统计


select t1.create_date,
       sum(case when t2.gender = '男' then t1.total_amount else 0 end) total_male,
       sum(case when t2.gender = '女' then t1.total_amount else 0 end) total_female
from (select create_date,
             user_id,
             sum(total_amount) total_amount
      from order_info
      group by create_date, user_id) t1
         join (select user_id,
                      gender
               from user_info) t2
              on t1.user_id = t2.user_id
group by t1.create_date;


//2.17 订单金额趋势分析

select create_date,
       t2.count_3d,
       round(t2.count_3d / num, 2) avg_ad
from (select create_date,
             case
                 when last = 1 and next = 1 then sum(total_1d)
                                                     over (order by create_date rows between 1 preceding and 1 following)
                 when last = 1 and next = 0 then sum(total_1d)
                                                     over (order by create_date rows between 1 preceding and current row )
                 when last = 0 and next = 1 then sum(total_1d)
                                                     over (order by create_date rows between current row and 1 following )
                 when last = 0 and next = 0 then sum(total_1d)
                                                     over (order by create_date rows between current row and current row ) end
                 as          count_3d,
             last + next + 1 num
      from (select create_date,
                   sum(price)                                                                                  total_1d,
                   if(lag(create_date, 1, null) over (order by create_date) = date_sub(create_date, 1), 1, 0)  last,
                   if(lead(create_date, 1, null) over (order by create_date) = date_add(create_date, 1), 1, 0) next
            from order_detail
            group by create_date) t1) t2;

//参考 错误
select create_date,
       round(sum(total_amount_by_day) over (order by create_date rows between 2 preceding and current row ),
             2)                                                                                                 total_3d,
       round(avg(total_amount_by_day) over (order by create_date rows between 2 preceding and current row ), 2) avg_3d
from (select create_date,
             sum(total_amount) total_amount_by_day
      from order_info
      group by create_date) t1;


//2.18 购买过商品1和商品2但是没有购买商品3的顾客

//5s
select user_id
from (select user_id,
             order_id
      from order_info) t1
         join (select order_id,
                      `if`(sku_id = 3, -3, sku_id) sku_id
               from order_detail
               where sku_id <= 3) t2
              on t1.order_id = t2.order_id
group by user_id
having sum(sku_id) = 3;


//参考 6s
select user_id
from (select user_id,
             collect_set(sku_id) skus
      from order_detail od
               left join
           order_info oi
           on od.order_id = oi.order_id
      group by user_id) t1
where array_contains(skus, '1')
  and array_contains(skus, '2')
  and !array_contains(skus, '3');


//2.19 统计每日商品1和商品2销量的差值

//3s
select nvl(t1.create_date, t2.create_date) create_date,
       nvl(t1.sum1, 0) - nvl(t2.sum2, 0)   diff
from (select create_date,
             sku_id,
             sum(sku_num) sum1
      from order_detail
      where sku_id = 1
      group by create_date, sku_id) t1
         full join (select create_date,
                           sku_id,
                           sum(sku_num) sum2
                    from order_detail
                    where sku_id = 2
                    group by create_date, sku_id) t2
                   on t1.create_date = t2.create_date

//参考 先执行第一个函数,第一个函数结束后执行下一个
select create_date,
       sum(if(sku_id = '1', sku_num, 0)) - sum(if(sku_id = '2', sku_num, 0)) diff
from order_detail
where sku_id in ('1', '2')
group by create_date;


//查询出每个用户的最近三笔订单
//1.6s
select user_id,
       order_id,
       create_date
from (select user_id,
             order_id,
             create_date,
             row_number() over (partition by user_id order by create_date desc ) rw
      from order_info) t1
where rw < 4;

//参考
select user_id,
       order_id,
       create_date
from (select user_id
           , order_id
           , create_date
           , row_number() over (partition by user_id order by create_date desc) rk
      from order_info) t1
where rk <= 3;


//2.21 查询每个用户登录日期的最大空档期

//效率同下 2.5s
select user_id,
       max(max_diff) max_diff
from (select user_id,
             login_ts,
             datediff(lead(login_ts, 1, '2021-10-10') over (partition by user_id order by login_ts), login_ts) max_diff
      from user_login_detail) t1
group by t1.user_id;

//2
select t2.user_id,
       t2.max_diff
from (select user_id,
             t1.max_diff,
             row_number() over (partition by user_id order by max_diff desc ) rw
      from (select user_id,
                   login_ts,
                   datediff(lead(login_ts, 1, '2021-10-10') over (partition by user_id order by login_ts),
                            login_ts) max_diff
            from user_login_detail) t1) t2
where t2.rw = 1;


//2.22 查询相同时刻多地登陆的用户

//1.232s
select user_id
from (select user_id,
             ip_address,
             dense_rank() over (partition by user_id order by ip_address) dr
      from user_login_detail
      group by user_id, ip_address) t1
where dr = 2;

//参考
select distinct t2.user_id
from (select t1.user_id,
             if(t1.max_logout is null, 2, if(t1.max_logout < t1.login_ts, 1, 0)) flag
      from (select user_id,
                   login_ts,
                   logout_ts,
                   max(logout_ts)
                       over (partition by user_id order by login_ts rows between unbounded preceding and 1 preceding) max_logout
            from user_login_detail) t1) t2
where t2.flag = 0;


//2.23 销售额完成任务指标的商品

//2.6s
select sku_id
from (select sku_id,
             month(create_date)                                                                               `month`,
             sum(price * sku_num)                                                                             count_num,
             if(sku_id = 1, `if`(sum(price * sku_num) > 21000, 1, 0), if(sum(price * sku_num) > 10000, 1, 0)) flag,
             row_number() over (partition by sku_id)                                                          rw
      from order_detail
      where sku_id < 3
      group by sku_id, month(create_date)) t1
group by sku_id, (month - rw)
having count(1) = 2;

//参考
select sku_id,
       concat(substring(create_date, 0, 7), '-01') ymd,
       sum(price * sku_num)                        sku_sum
from order_detail
where sku_id = 1
   or sku_id = 2
group by sku_id, substring(create_date, 0, 7)
having (sku_id = 1 and sku_sum >= 21000)
    or (sku_id = 2 and sku_sum >= 10000)

-- 判断是否为连续两个月
select distinct t3.sku_id
from (select t2.sku_id,
             count(*) over (partition by t2.sku_id,t2.rymd) cn
      from (select t1.sku_id,
                   add_months(t1.ymd, -row_number() over (partition by t1.sku_id order by t1.ymd)) rymd
            from (select sku_id,
                         concat(substring(create_date, 0, 7), '-01') ymd,
                         sum(price * sku_num)                        sku_sum
                  from order_detail
                  where sku_id = 1
                     or sku_id = 2
                  group by sku_id, substring(create_date, 0, 7)
                  having (sku_id = 1 and sku_sum >= 21000)
                      or (sku_id = 2 and sku_sum >= 10000)) t1) t2) t3
where t3.cn >= 2;


//2.24 根据商品销售情况进行商品分类

//2.5s
select Category,
       count(1) cn
from (select case
                 when sum(sku_num) between 0 and 5000 then '冷门商品'
                 when sum(sku_num) between 5001 and 19999 then '一般商品'
                 when sum(sku_num) > 19999 then '热门商品' end as Category
      from order_detail
      group by sku_id) t1
group by Category;


//参考 2.5s
select t2.category,
       count(*) cn
from (select t1.sku_id,
             case
                 when t1.sku_sum >= 0 and t1.sku_sum <= 5000 then '冷门商品'
                 when t1.sku_sum >= 5001 and t1.sku_sum <= 19999 then '一般商品'
                 when t1.sku_sum >= 20000 then '热门商品'
                 end category
      from (select sku_id,
                   sum(sku_num) sku_sum
            from order_detail
            group by sku_id) t1) t2
group by t2.category;



//2.25 各品类销量前三的商品

//6s
select sku_id,
       category_id
from (select t1.sku_id,
             t1.category_id,
             row_number() over (partition by category_id order by sku_num desc ) rw
      from sku_info t1
               join (select sku_id,
                            sum(sku_num) sku_num
                     from order_detail
                     group by sku_id) t2
                    on t1.sku_id = t2.sku_id) t3
where rw < 4;


//参考
select t2.sku_id,
       t2.category_id
from (select t1.sku_id,
             si.category_id,
             rank() over (partition by category_id order by t1.sku_sum desc) rk
      from (select sku_id,
                   sum(sku_num) sku_sum
            from order_detail
            group by sku_id) t1
               join
           sku_info si
           on
               t1.sku_id = si.sku_id) t2
where t2.rk <= 3;



//2.26 各品类中商品价格的中位数

//3s

select t2.category_id,
       if(collect_set(t2.one)[0] = collect_set(t2.two)[0], collect_set(t2.price)[0], sum(t2.price) / 2) medprice
from (select category_id,
             price,
             rw,
             ceil((first_value(rw, false) over (partition by category_id) +
                   last_value(rw, false) over (partition by category_id)) / 2)    one,
             `floor`((first_value(rw, false) over (partition by category_id) +
                      last_value(rw, false) over (partition by category_id)) / 2) two
      from (select category_id,
                   price,
                   row_number() over (partition by category_id order by price) rw,
                   count(1) over (partition by category_id) % 2                falg
            from sku_info) t1) t2
where t2.rw = t2.one
   or t2.rw = t2.two
group by t2.category_id;

//参考 8s

select distinct t1.category_id,
                avg(t1.price) over (partition by t1.category_id) medprice
from (select sku_id,
             category_id,
             price,
             row_number() over (partition by category_id order by price desc) rk,
             count(*) over (partition by category_id)                         cn,
             count(*) over (partition by category_id) % 2                     falg
      from sku_info) t1
where t1.falg = 0
  and (t1.rk = cn / 2 or t1.rk = cn / 2 + 1)

union

select t1.category_id,
       t1.price / 1
from (select sku_id,
             category_id,
             price,
             row_number() over (partition by category_id order by price desc) rk,
             count(*) over (partition by category_id)                         cn,
             count(*) over (partition by category_id) % 2                     falg
      from sku_info) t1
where t1.falg = 1
  and t1.rk = round(cn / 2);


//2.27 找出销售额连续3天超过100的商品

//2.6s
select sku_id
from (select sku_id,
             date_sub(create_date, rw) flag
      from (select sku_id,
                   create_date,
                   sum(sku_num * price)                                         total_1d,
                   row_number() over (partition by sku_id order by create_date) rw
            from order_detail
            group by sku_id, create_date) t1
      where t1.total_1d > 100) t2
group by t2.sku_id, t2.flag
having count(*) >= 3;

select sku_id
from order_detail
group by sku_id;


//2.28 查询有新注册用户的当天的新用户数量、新用户的第一天留存率


//4s
select login_ts,
       register,
       round(num / register, 2) Ratention
from (select t1.login_ts,
             count(if(t1.rw = 1, 1, null)) over (partition by t1.login_ts) register,
             sum(if(date_add(login_ts, 1) = lead(login_ts, 1, null) over (partition by user_id order by login_ts), 1,
                    0)) over (partition by login_ts)                       num,
             row_number() over (partition by login_ts)                     rw1
      from (select user_id,
                   substr(login_ts, 1, 10)                                    login_ts,
                   row_number() over (partition by user_id order by login_ts) rw
            from user_login_detail) t1
      where rw < 3) t2
where register > 0
  and rw1 = 1;


//2.29 求出商品连续售卖的时间区间

//3s
select sku_id,
       Start_date,
       End_date
from (select sku_id,
             first_value(create_date, false)
                         over (partition by sku_id,flag order by create_date rows between unbounded preceding and unbounded following) Start_date,
             last_value(create_date, false)
                        over (partition by sku_id,flag order by create_date rows between unbounded preceding and unbounded following)  End_date
      from (select sku_id,
                   create_date,
                   date_sub(create_date, rw) flag
            from (select sku_id,
                         create_date,
                         row_number() over (partition by sku_id order by create_date) rw
                  from order_detail
                  group by sku_id, create_date) t1) t2) t3
group by sku_id, Start_date, End_date;


//2.30 登录次数及交易次数统计

//5s
select t1.user_id,
       t1.login_ts,
       t1.login_count,
       nvl(t2.Order_count, 0) order_count
from (select user_id,
             substr(login_ts, 1, 10) login_ts,
             count(*)                login_count
      from user_login_detail
      group by user_id, substr(login_ts, 1, 10)) t1
         left join
     (select user_id,
             order_date,
             count(*) Order_count
      from delivery_info
      group by user_id, order_date) t2
     on t1.user_id = t2.user_id and t1.login_ts = t2.order_date;


//2.31 按年度列出每个商品销售总额

//1.3s
select sku_id,
       year(create_date)    Year_date,
       sum(sku_num * price) Sku_sum
from order_detail
group by sku_id, year(create_date);


//2.32. 某周内每件商品每天销售情况

//1.4s
select t1.sku_id,
       sum(if(create_date = '2021-09-27', total_num, 0)) Monday,
       sum(if(create_date = '2021-09-28', total_num, 0)) Tuesdayday,
       sum(if(create_date = '2021-09-29', total_num, 0)) Wednesdayday,
       sum(if(create_date = '2021-09-30', total_num, 0)) Thursday,
       sum(if(create_date = '2021-10-01', total_num, 0)) Friday,
       sum(if(create_date = '2021-10-02', total_num, 0)) Saturday,
       sum(if(create_date = '2021-10-03', total_num, 0)) Sunday
from (select sku_id,
             create_date,
             sum(sku_num) total_num
      from order_detail
      where create_date >= '2021-09-27'
        and create_date <= '2021-10-03'
      group by sku_id, create_date) t1
group by t1.sku_id;


//参考
select sku_id,
       sum(if(dayofweek(create_date) = 2, sku_num, 0)) Monday,
       sum(if(dayofweek(create_date) = 3, sku_num, 0)) Tuesday,
       sum(if(dayofweek(create_date) = 4, sku_num, 0)) Wednesday,
       sum(if(dayofweek(create_date) = 5, sku_num, 0)) Thursday,
       sum(if(dayofweek(create_date) = 6, sku_num, 0)) Friday,
       sum(if(dayofweek(create_date) = 7, sku_num, 0)) Saturday,
       sum(if(dayofweek(create_date) = 1, sku_num, 0)) Sunday
from order_detail
where create_date >= '2021-09-27'
  and create_date <= '2021-10-03'
group by sku_id;


//2.33 查看每件商品的售价涨幅情况

//1.2s
select sku_id,
       new_price - old_price Price_change
from (select sku_id,
             new_price,
             change_date,
             lead(new_price, 1, new_price) over (partition by sku_id order by change_date desc ) old_price,
             row_number() over (partition by sku_id order by change_date desc )                  rw
      from sku_price_modify_detail) t1
where rw = 1;


//2.34 销售订单首购和次购分析

//5s
select t3.user_id,
       collect_set(t2.create_date)[0]                                     First_date,
       collect_set(t2.create_date)[size(collect_set(t2.create_date)) - 1] Last_date,
       count(*)                                                           Cn
from sku_info t1
         join order_detail t2
              on t1.sku_id = t2.sku_id
         join order_info t3
              on t3.order_id = t2.order_id
where name in ('xiaomi 10', 'apple 12', 'xiaomi 13')
group by t3.user_id;

//参考 6s
select distinct oi.user_id,
                first_value(od.create_date)
                            over (partition by oi.user_id order by od.create_date rows between unbounded preceding and unbounded following ) first_date,
                last_value(od.create_date)
                           over (partition by oi.user_id order by od.create_date rows between unbounded preceding and unbounded following )  last_date,
                count(*)
                      over (partition by oi.user_id order by od.create_date rows between unbounded preceding and unbounded following)        cn
from order_info oi
         join
     order_detail od
     on
         oi.order_id = od.order_id
         join
     sku_info si
     on
         od.sku_id = si.sku_id
where si.name in ('xiaomi 10', 'apple 12', 'xiaomi 13');


//2.35 同期商品售卖分析表

//求出同一个商品在2020年和2021年中同一个月的售卖情况对比

//1.25s
select sku_id,
       month,
       collect_set(sku_num)[0]                                         2020_num,
       if(collect_set(sku_num)[1] is null, 0, collect_set(sku_num)[1]) 2021_num
from (select sku_id,
             month(create_date) month,
             year(create_date)  year,
             sum(sku_num)       sku_num
      from order_detail
      group by sku_id, month(create_date), year(create_date)) t1
group by sku_id, month;


//参考
//两个年份表分开
select if(t1.sku_id is null, t2.sku_id, t1.sku_id),
       month(if(t1.ym is null, t2.ym, t1.ym)),
       if(t1.sku_sum is null, 0, t1.sku_sum) 2020_skusum,
       if(t2.sku_sum is null, 0, t2.sku_sum) 2021_skusum
from (select sku_id,
             concat(date_format(create_date, 'yyyy-MM'), '-01') ym,
             sum(sku_num)                                       sku_sum
      from order_detail
      where year(create_date) = 2020
      group by sku_id, date_format(create_date, 'yyyy-MM')) t1
         full join
     (select sku_id,
             concat(date_format(create_date, 'yyyy-MM'), '-01') ym,
             sum(sku_num)                                       sku_sum
      from order_detail
      where year(create_date) = 2021
      group by sku_id, date_format(create_date, 'yyyy-MM')) t2
     on
         t1.sku_id = t2.sku_id and month(t1.ym) = month(t2.ym);


//2.36 国庆期间每个品类的商品的收藏量和购买量

//6s
select t1.sku_id,
       t1.Sku_sum,
       nvl(t2.favor_cn, 0) Favor_cn
from (select sku_id,
             sum(sku_num) Sku_sum
      from order_detail
      where create_date <= '2021-10-07'
        and create_date >= '2021-10-01'
      group by sku_id) t1
         left join (select sku_id,
                           count(*) favor_cn
                    from favor_info
                    where create_date <= '2021-10-07'
                      and create_date >= '2021-10-01'
                    group by sku_id) t2
                   on t1.sku_id = t2.sku_id;


//参考
select t1.sku_id,
       t1.sku_sum,
       t2.favor_cn
from (select sku_id,
             sum(sku_num) sku_sum
      from order_detail
      where create_date >= '2021-10-01'
        and create_date <= '2021-10-07'
      group by sku_id) t1
         join
     (select sku_id,
             count(*) favor_cn
      from favor_info
      where create_date >= '2021-10-01'
        and create_date <= '2021-10-07'
      group by sku_id) t2
     on
         t1.sku_id = t2.sku_id;


//2.37 统计活跃间隔对用户分级结果

//4s
select level,
       count(*) Cn
from (select if(first > date_sub(today, 7), '新晋用户', if(datediff(today, last) <= 7, '忠实用户',
                                                           if(datediff(today, last) <= 30, '沉睡用户', '流失用户'))) level
      from (select user_id,
                   min(login_ts) over (partition by user_id)                  first,
                   max(logout_ts) over (partition by user_id)                 last,
                   max(logout_ts) over ()                                     today,
                   row_number() over (partition by user_id order by login_ts) rw
            from user_login_detail) t1
      where rw = 1) t2
group by level;


//2.38 连续签到领金币数

//3.5s
select user_id,
       sum(coin_cn) Sum_coin_cn
from (select user_id,
             if(count(*) >= 7, count(*) / 7 * 6 + count(*) % 7, if(count(*) >= 3, count(*) + 2, count(*))) coin_cn
      from (select t1.user_id,
                   t1.login_ts,
                   date_sub(t1.login_ts, t1.rk) date_count
            from (select user_id,
                         substr(login_ts, 1, 10)                                             login_ts,
                         rank() over (partition by user_id order by substr(login_ts, 1, 10)) rk
                  from user_login_detail
                  group by user_id, substr(login_ts, 1, 10)) t1) t2
      group by user_id, date_count) t3
group by user_id
order by Sum_coin_cn desc;


//参考 4.3s
select t3.user_id,
       sum(t3.coin_cn) sum_coin_cn
from (select t2.user_id,
             max(t2.counti_cn) + sum(if(t2.counti_cn % 3 = 0, 2, 0)) + sum(if(t2.counti_cn % 7 = 0, 6, 0)) coin_cn
      from (select t1.user_id,
                   t1.login_date,
                   date_sub(t1.login_date, t1.rk)                                                              login_date_rk,
                   count(*)
                         over (partition by t1.user_id, date_sub(t1.login_date, t1.rk) order by t1.login_date) counti_cn
            from (select user_id,
                         date_format(login_ts, 'yyyy-MM-dd')                                             login_date,
                         rank() over (partition by user_id order by date_format(login_ts, 'yyyy-MM-dd')) rk
                  from user_login_detail
                  group by user_id, date_format(login_ts, 'yyyy-MM-dd')) t1) t2
      group by t2.user_id, t2.login_date_rk) t3
group by t3.user_id
order by sum_coin_cn desc;


//2.39 国庆期间的7日动销率和滞销率

select
    t4.category_id,
    `第1天`/t5.cn `1号(动销)`,
    (t5.cn-`第1天`)/t5.cn `1号(滞销)`,
    `第2天`/t5.cn `2号(动销)`,
    (t5.cn-`第2天`)/t5.cn `2号(滞销)`,
    `第3天`/t5.cn `3号(动销)`,
    (t5.cn-`第3天`)/t5.cn `3号(滞销)`,
    `第4天`/t5.cn `4号(动销)`,
    (t5.cn-`第1天`)/t5.cn `4号(滞销)`,
    `第5天`/t5.cn `5号(动销)`,
    (t5.cn-`第5天`)/t5.cn `5号(滞销)`,
    `第6天`/t5.cn `6号(动销)`,
    (t5.cn-`第6天`)/t5.cn `6号(滞销)`,
    `第7天`/t5.cn `7号(动销)`,
    (t5.cn-`第7天`)/t5.cn `7号(滞销)`
from (select
    t3.category_id,
    sum(if(t3.create_date='2021-10-01',1,0)) `第1天`,
    sum(if(t3.create_date='2021-10-02',1,0)) `第2天`,
    sum(if(t3.create_date='2021-10-03',1,0)) `第3天`,
    sum(if(t3.create_date='2021-10-04',1,0)) `第4天`,
    sum(if(t3.create_date='2021-10-05',1,0)) `第5天`,
    sum(if(t3.create_date='2021-10-06',1,0)) `第6天`,
    sum(if(t3.create_date='2021-10-07',1,0)) `第7天`
from (select collect_set(t2.category_id)[0] category_id,
             substr(create_date, 1, 10) create_date
      from order_detail t1
               join sku_info t2
                    on t1.sku_id = t2.sku_id
      where substr(create_date, 1, 10) >= '2021-10-01'
        and substr(create_date, 1, 10) <= '2021-10-07'
      group by t1.sku_id, substr(create_date, 1, 10)) t3
group by t3.category_id) t4 join (select category_id,
       count(*) cn
from sku_info
group by category_id) t5
on t4.category_id=t5.category_id;


//