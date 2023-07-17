//1、查询订单明细表（order_detail）中销量（下单件数）排名第二的商品id，如果不存在返回null，如果存在多个排名第二的商品则需要全部返回。

select sku_id
from (select sku_id
      from (select sku_id,
                   dense_rank() over (order by sum(sku_num) desc) dr
            from order_detail
            group by sku_id) t1
      where t1.dr = 2) t2
         full outer join (select 1 flag) t3;


//2、查询订单信息表(order_info)中最少连续3天下单的用户id
select user_id
from (select user_id,
             create_date,
             row_number() over (partition by user_id order by create_date) rn
      from order_info
      group by user_id, create_date) t1
group by user_id, date_sub(create_date, rn)
having count(*) > 2;

//3、从订单明细表(order_detail)统计各品类销售出的商品种类数及累积销量最好的商品
select t3.category_id,
       t4.category_name,
       sku_id,
       name,
       num,
       sku_cnt
from (select category_id,
             t1.sku_id,
             name,
             num,
             row_number() over (partition by category_id order by num desc ) rn,
             count(t1.sku_id) over (partition by category_id)                sku_cnt
      from (select sku_id,
                   sum(sku_num) num
            from order_detail
            group by sku_id) t1
               join (select sku_id,
                            name,
                            category_id
                     from sku_info) t2
                    on t1.sku_id = t2.sku_id) t3
         join (select *
               from category_info) t4 on t3.category_id = t4.category_id
where t3.rn = 1;


//4、从订单信息表(order_info)中统计每个用户截止其每个下单日期的累积消费金额，以及每个用户在其每个下单日期的VIP等级。

select user_id,
       create_date,
       num        sum_so_far,
       case
           when num >= 100000 then '钻石会员'
           when num >= 80000 then '白金会员'
           when num >= 50000 then '黄金会员'
           when num >= 30000 then '白银会员'
           when num >= 10000 then '青铜会员'
           when num >= 0 then '普通会员'
           end as vip_level
from (select user_id,
             create_date,
             sum(num) over (partition by user_id order by create_date) num
      from (select user_id,
                   create_date,
                   sum(total_amount) num
            from order_info
            group by user_id, create_date) t1) t2


//5、从订单信息表(order_info)中查询首次下单后第二天仍然下单的用户占所有下单用户的比例，结果保留一位小数，使用百分数显示

;
select concat(count(t2.user_id) / count(t3.user_id) * 100, '%') percentage
from (select user_id
      from (select user_id,
                   create_date,
                   row_number() over (partition by user_id order by create_date) rn
            from order_info
            group by user_id, create_date) t1
      where t1.rn <= 2
      group by user_id, date_sub(create_date, t1.rn)
      having count(*) = 2) t2
         right join (select user_id
                     from order_info
                     group by user_id) t3 on t2.user_id = t3.user_id;


//6、从订单明细表(order_detail)统计每个商品销售首年的年份，销售数量和销售总额

select sku_id,
       year,
       order_num,
       order_amount
from (select *,
             row_number() over (partition by sku_id order by year) rn
      from (select sku_id,
                   year(create_date)    year,
                   sum(sku_num)         order_num,
                   sum(price * sku_num) order_amount
            from order_detail
            group by sku_id, year(create_date)) t1) t2
where rn = 1
;


//7、从订单明细表(order_detail)中筛选出去年总销量小于100的商品及其销量，假设今天的日期是2022-01-10，不考虑上架时间小于一个月的商品

select t1.sku_id,
       name,
       sum(sku_num) order_num
from order_detail t1
         join (select sku_id,
                      name
               from sku_info
               where from_date < '2021-12-01') t2 on t1.sku_id = t2.sku_id
where year(create_date) = '2021'
group by t1.sku_id, name
having sum(sku_num) < 100;


//8、从用户登录明细表（user_login_detail）中查询每天的新增用户数，若一个用户在某天登录了，且在这一天之前没登录过，则任务该用户为这一天的新增用户。
select login_date_first,
       count(user_id) user_count
from (select user_id,
             date_format(min(login_ts), 'yyyy-MM-dd') login_date_first
      from user_login_detail
      group by user_id) t1
group by login_date_first;

//9、从订单明细表（order_detail）中统计出每种商品销售件数最多的日期及当日销量，如果有同一商品多日销量并列的情况，取其中的最小日期。
select sku_id,
       create_date,
       sum_num
from (select sku_id,
             create_date,
             sum(sku_num)                                                                        sum_num,
             row_number() over (partition by sku_id order by sum(sku_num) desc,create_date asc ) rn
      from order_detail
      group by sku_id, create_date) t1
where rn = 1;


//10、从订单明细表（order_detail）中查询累积销售件数高于其所属品类平均数的商品

select *
from (select sku_id,
             name,
             sum_num,
             cast(avg(sum_num) over (partition by category_id) as bigint) cate_avg_num
      from (select category_id,
                   od.sku_id,
                   name,
                   sum(sku_num) sum_num
            from order_detail od
                     join sku_info si
                          on od.sku_id = si.sku_id
            group by category_id, od.sku_id, name) t1) t2
where sum_num > cate_avg_num;


//11、从用户登录明细表（user_login_detail）和订单信息表（order_info）中查询每个用户的注册日期（首次登录日期）、总登录次数以及其在2021年的登录次数、订单数和订单总额。

select t1.user_id,
       register_date,
       total_login_count,
       login_count_2021,
       order_count_2021,
       order_amount_2021
from (select user_id,
             date_format(min(login_ts), 'yyyy-MM-dd')    register_date,
             count(login_ts)                             total_login_count,
             count(`if`(year(login_ts) = 2021, 1, null)) login_count_2021
      from user_login_detail
      group by user_id) t1
         join (select user_id,
                      count(order_id)   order_count_2021,
                      sum(total_amount) order_amount_2021
               from order_info
               where year(create_date) = '2021'
               group by user_id) t2 on t1.user_id = t2.user_id;


//12、查询所有商品（sku_info表）截至到2021年10月01号的最新商品价格（需要结合价格修改表进行分析

select sku_id,
       cast(price as decimal(16, 2)) price
from (select sku_id,
             price,
             row_number() over (partition by sku_id order by from_date desc ) rn
      from (select sku_id,
                   price,
                   from_date
            from sku_info
            union
            select sku_id,
                   new_price,
                   change_date
            from sku_price_modify_detail
            where change_date <= '2021-10-01') t1) t2
where rn = 1;



//13、订单配送中，如果期望配送日期和下单日期相同，称为即时订单，如果期望配送日期和下单日期不同，称为计划订单。
//请从配送信息表（delivery_info）中求出每个用户的首单（用户的第一个订单）中即时订单的比例，保留两位小数，以小数形式显示。
select cast(count(`if`(order_date = custom_date, 1, null)) / count(user_id) as decimal(16, 2)) percentage
from (select user_id,
             order_id,
             order_date,
             custom_date,
             row_number() over (partition by user_id order by order_id) rn
      from delivery_info) t1
where t1.rn = 1;


//14、现需要请向所有用户推荐其朋友收藏但是用户自己未收藏的商品，请从好友关系表（friendship_info）和收藏表（favor_info）中查询出应向哪位用户推荐哪些商品。
select t2.user1_id user_id,
       t2.sku_id
from (select t0.user1_id,
             sku_id
      from friendship_info t0
               join (select user_id,
                            sku_id
                     from favor_info) t1 on t0.user2_id = t1.user_id
      group by t0.user1_id, sku_id) t2
         left join (select user_id,
                           sku_id
                    from favor_info) t3 on t2.user1_id = t3.user_id
    and t2.sku_id = t3.sku_id
where t3.sku_id is null;

select t5.user_id,
       sku_id
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
        and t4.sku_id is null) t5;

//15、从登录明细表（user_login_detail）中查询出，所有用户的连续登录两天及以上的日期区间，以登录时间（login_ts）为准。


select user_id,
       ld                            start_date,
       date_add(ld, cast(rn as int)) end_date
from (select user_id,
             date_sub(login_date, cast(sum_rn as int)) ld,
             count(user_id)                            rn
      from (select *,
                   sum(if(login_date = date_add(last_date, 1), 1, 0))
                       over (partition by user_id order by login_date) sum_rn
            from (select user_id,
                         login_date,
                         lag(login_date, 1, null) over (partition by user_id order by login_date) last_date
                  from (select user_id,
                               date_format(login_ts, 'yyyy-MM-dd') login_date
                        from user_login_detail
                        group by user_id, date_format(login_ts, 'yyyy-MM-dd')) t1) t2) t3
      where t3.sum_rn >= 1
      group by user_id, date_sub(login_date, cast(sum_rn as int))) t4
;

//16、从订单信息表（order_info）和用户信息表（user_info）中，分别统计每天男性和女性用户的订单总金额，如果当天男性或者女性没有购物，则统计结果为0

select create_date,
       sum(one) total_amount_male,
       sum(two) total_amount_female
from (select create_date,
             sum(`if`(gender = '男', total_amount, 0.00)) one,
             sum(`if`(gender = '女', total_amount, 0.00)) two
      from user_info ui
               join order_info oi
                    on oi.user_id = ui.user_id
      group by create_date, gender) t1
group by create_date


//17、查询截止每天的最近3天内的订单金额总和以及订单金额日平均值，保留两位小数，四舍五入。
;

select create_date,
       nvl(t1_num, 0.00) + nvl(t2_num, 0.00) + nvl(t3_num, 0.00)                                     total_3d,
       cast((nvl(t1_num, 0.00) + nvl(t2_num, 0.00) + nvl(t3_num, 0.00)) /
            (`if`(t2_num is not null, 1, 0) + `if`(t3_num is not null, 1, 0) + 1) as decimal(16, 2)) avg_3d
from (select t1.create_date,
             t1.sum_num t1_num,
             t2.sum_num t2_num,
             t3.sum_num t3_num
      from (select create_date,
                   sum(total_amount) sum_num
            from order_info
            group by create_date) t1
               left join (select create_date,
                                 sum(total_amount) sum_num
                          from order_info
                          group by create_date) t2 on date_sub(t1.create_date, 1) = t2.create_date
               left join (select create_date,
                                 sum(total_amount) sum_num
                          from order_info
                          group by create_date) t3 on date_sub(t1.create_date, 2) = t3.create_date) t4;


//18、从订单明细表(order_detail)中查询出所有购买过商品1和商品2，但是没有购买过商品3的用户，


select user_id
from (select oi.user_id,
             collect_set(od.sku_id) num
      from order_info oi
               join (select order_id,
                            sku_id
                     from order_detail
                     where sku_id <= 3) od on od.order_id = oi.order_id
      group by oi.user_id) t1
where array_contains(num, '1')
  and array_contains(num, '2')
  and array_contains(num, '3') = false;


//19、从订单明细表（order_detail）中统计每天商品1和商品2销量（件数）的差值（商品1销量-商品2销量）
select create_date,
       sum(`if`(sku_id = '1', sum_num, 0)) - sum(`if`(sku_id = '2', sum_num, 0)) diff
from (select create_date,
             sku_id,
             sum(sku_num) sum_num
      from order_detail
      where sku_id <= 2
      group by create_date, sku_id) t1
group by create_date;


//20、从订单信息表（order_info）中查询出每个用户的最近三个下单日期的所有订单

select user_id,
       order_id,
       create_date
from (select user_id,
             order_id,
             create_date,
             dense_rank() over (partition by user_id order by create_date desc) rn
      from order_info) t1
where rn <= 3;

//21、从登录明细表（user_login_detail）中查询每个用户两个登录日期（以login_ts为准）之间的最大的空档期。统计最大空档期时，
-- 用户最后一次登录至今的空档也要考虑在内，假设今天为2021-10-10。
select user_id,
       max(diff) max_diff
from (select user_id,
             datediff(lead(date_format(login_ts, 'yyyy-MM-dd'), 1, '2021-10-10')
                           over (partition by user_id order by date_format(login_ts, 'yyyy-MM-dd')),
                      date_format(login_ts, 'yyyy-MM-dd')) diff
      from user_login_detail
      group by user_id, date_format(login_ts, 'yyyy-MM-dd')) t1
group by user_id;


//22、从登录明细表（user_login_detail）中查询在相同时刻，多地登陆（ip_address不同）的用户
select distinct user_id
from (select user_id,
             ip_address,
             op,
             is_num,
             is_num_next,
             ip
      from (select user_id,
                   ip_address,
                   op,
                   is_num,
                   lead(is_num, 1, 0) over (partition by user_id order by op)        is_num_next,
                   lead(ip_address, 1, null) over (partition by user_id order by op) ip
            from (select user_id,
                         ip_address,
                         op,
                         sum(flag) over (partition by user_id order by op) is_num
                  from (select user_id,
                               ip_address,
                               login_ts op,
                               1        flag
                        from user_login_detail
                        union
                        select user_id,
                               ip_address,
                               logout_ts op,
                               -1        flag
                        from user_login_detail) t1) t2
            where is_num > 0) t3
      where is_num < is_num_next
        and ip != ip_address) t4;


//23、

/*
        商家要求每个商品每个月需要售卖出一定的销售总额
假设1号商品销售总额大于21000，2号商品销售总额大于10000，其余商品没有要求
请写出SQL从订单详情表中（order_detail）查询连续两个月销售总额大于等于任务总额的商品
 */
select sku_id
from (select sku_id,
             month - rn dt
      from (select sku_id,
                   month(create_date)                                                                    month,
                   row_number() over (partition by sku_id,year(create_date) order by month(create_date)) rn
            from order_detail
            where sku_id <= 2
            group by sku_id, year(create_date), month(create_date)
            having (sku_id = 1 and sum(price * sku_num) >= 21000)
                or (sku_id = 2 and sum(price * sku_num) >= 10000)) t1) t2
group by sku_id, dt
having count(sku_id) >= 2;



//24、

/*
    从订单详情表中（order_detail）对销售件数对商品进行分类，0-5000为冷门商品，5001-19999位一般商品，20000往上为热门商品，并求出不同类别商品的数量
 */

select category,
       count(category) cn
from (select sku_id,
             case
                 when sum(sku_num) >= 20000 then '热门商品'
                 when sum(sku_num) >= 5001 then '一般商品'
                 when sum(sku_num) >= 0 then '冷门商品' end as category
      from order_detail
      group by sku_id) t1
group by category;


//25、从订单详情表中（order_detail）和商品（sku_info）中查询各个品类销售数量前三的商品。如果该品类小于三个商品，则输出所有的商品销量。

select sku_id,
       category_id
from (select t1.sku_id,
             t2.category_id,
             t1.sum_num,
             dense_rank() over (partition by category_id order by sum_num desc ) dr
      from (select sku_id,
                   sum(sku_num) sum_num
            from order_detail
            group by sku_id) t1
               join (select sku_id,
                            category_id
                     from sku_info) t2 on t1.sku_id = t2.sku_id) t3
where t3.dr <= 3;


//26、从商品信息表（sku_info）求出各分类商品价格的中位数，如果一个分类下的商品个数为偶数则输出中间两个值的平均值，如果是奇数，则输出中间数即可。

select category_id,
       cast(avg(price) as decimal(16, 2)) medprice
from (select category_id,
             price,
             row_number() over (partition by category_id order by price)                          rn,
             `if`((count(category_id) over (partition by category_id)) % 2 = 0,
                  array(cast((count(category_id) over (partition by category_id) / 2) as int),
                        cast((count(category_id) over (partition by category_id)) / 2 + 1 as int)),
                  `array`(cast((count(category_id) over (partition by category_id)) / 2 as int))) flag
      from sku_info) t1
         lateral view explode(flag) tmp as num
where rn = num
group by category_id;


//27、从订单详情表（order_detail）中找出销售额连续3天超过100的商品

select sku_id
from (select sku_id,
             date_sub(create_date, rn) flag
      from (select sku_id,
                   create_date,
                   sum(sku_num * price)                                         sum_num,
                   row_number() over (partition by sku_id order by create_date) rn
            from order_detail
            group by sku_id, create_date
            having sum_num >= 100) t1) t2
group by sku_id, flag
having count(flag) >= 3;


//28、从用户登录明细表（user_login_detail）中首次登录算作当天新增，第二天也登录了算作一日留存

select login_date                       first_login,
       count(user_id)                   register,
       cast(avg(num) as decimal(16, 2)) retention
from (select login_date,
             user_id,
             `if`(datediff(next, login_date) = 1, 1, 0) num
      from (select user_id,
                   date_format(login_ts, 'yyyy-MM-dd')                                                   login_date,
                   lead(date_format(login_ts, 'yyyy-MM-dd'), 1, '9999-12-31')
                        over (partition by user_id order by date_format(login_ts, 'yyyy-MM-dd'))         next,
                   row_number() over (partition by user_id order by date_format(login_ts, 'yyyy-MM-dd')) rn
            from user_login_detail
            group by user_id, date_format(login_ts, 'yyyy-MM-dd')) t1
      where rn = 1) t2
group by login_date;


//29、从订单详情表（order_detail）中，求出商品连续售卖的时间区间


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
                   date_sub(create_date, rn) flag
            from (select sku_id,
                         create_date,
                         row_number() over (partition by sku_id order by create_date) rn
                  from order_detail
                  group by sku_id, create_date) t1) t2) t3
group by sku_id, Start_date, End_date;


select sku_id,
       Start_date,
       End_date
from (select sku_id,
             min(create_date) Start_date,
             max(create_date) End_date
      from (select sku_id,
                   create_date,
                   date_sub(create_date, rw) flag
            from (select sku_id,
                         create_date,
                         row_number() over (partition by sku_id order by create_date) rw
                  from order_detail
                  group by sku_id, create_date) t1) t2
      group by sku_id, flag) t3
group by sku_id, Start_date, End_date;


//30、分别从登陆明细表（user_login_detail）和配送信息表中用户登录时间和下单时间统计登陆次数和交易次数
select t1.user_id,
       login_date,
       login_count,
       nvl(order_count, 0) order_count
from (select user_id,
             date_format(login_ts, 'yyyy-MM-dd') login_date,
             count(user_id)                      login_count
      from user_login_detail
      group by user_id, date_format(login_ts, 'yyyy-MM-dd')) t1
         left join
     (select user_id,
             order_date,
             count(order_date) order_count
      from delivery_info
      group by user_id, order_date) t2
     on t1.user_id = t2.user_id and t1.login_date = t2.order_date;


//31、 从订单明细表（order_detail）中列出每个商品每个年度的购买总额

select sku_id,
       year(create_date)    year_date,
       sum(price * sku_num) sku_sum
from order_detail
group by sku_id, year(create_date);


//32、 从订单详情表（order_detail）中查询2021年9月27号-2021年10月3号这一周所有商品每天销售情况。

select sku_id,
       sum(`if`(create_date = '2021-09-27', sku_num, 0)) monday,
       sum(`if`(create_date = '2021-09-28', sku_num, 0)) tuesday,
       sum(`if`(create_date = '2021-09-29', sku_num, 0)) wednesday,
       sum(`if`(create_date = '2021-09-30', sku_num, 0)) thursday,
       sum(`if`(create_date = '2021-10-01', sku_num, 0)) friday,
       sum(`if`(create_date = '2021-10-02', sku_num, 0)) saturday,
       sum(`if`(create_date = '2021-10-03', sku_num, 0)) sunday
from order_detail
where create_date between '2021-09-27' and '2021-10-03'
group by sku_id;


//33、从商品价格变更明细表（sku_price_modify_detail），得到最近一次价格的涨幅情况，并按照涨幅升序排序。

select sku_id,
       price_change
from (select sku_id,
             new_price -
             lead(new_price, 1, new_price) over (partition by sku_id order by change_date desc) price_change,
             row_number() over (partition by sku_id order by change_date desc )                 rn
      from sku_price_modify_detail) t1
where rn = 1
order by price_change;


//34、

/*
        通过商品信息表（sku_info）订单信息表（order_info）订单明细表（order_detail）
        分析如果有一个用户成功下单两个及两个以上的购买成功的手机订单（购买商品为xiaomi 10，apple 12，小米13）
        那么输出这个用户的id及第一次成功购买手机的日期和第二次成功购买手机的日期，以及购买手机成功的次数。
 */


select user_id,
       min(od.create_date) first_date,
       max(od.create_date) last_date,
       count(user_id)      cn
from order_detail od
         join sku_info si
              on od.sku_id = si.sku_id
         join order_info oi
              on oi.order_id = od.order_id
where name in ('xiaomi 10', 'xiaomi 13', 'apple 12')
group by user_id;


//35、从订单明细表（order_detail）中。 求出同一个商品在2021年和2022年中同一个月的售卖情况对比。

select sku_id,
       month,
       sum(`if`(year = '2020', num, 0)) 2020_skusum,
       sum(`if`(year = '2021', num, 0)) 2021_skusum
from (select sku_id,
             year(create_date)  year,
             month(create_date) month,
             sum(sku_num)       num
      from order_detail
      group by sku_id, year(create_date), month(create_date)) t1
group by sku_id, month;


//36、从订单明细表（order_detail）和收藏信息表（favor_info）统计2021国庆期间，每个商品总收藏量和购买量
select t1.sku_id,
       sku_sum,
       nvl(favor_cn, 0) favor_cn
from (select sku_id,
             sum(sku_num) sku_sum
      from order_detail
      where create_date between '2021-10-01' and '2021-10-07'
      group by sku_id) t1
         left join
     (select sku_id,
             count(sku_id) favor_cn
      from favor_info
      where create_date between '2021-10-01' and '2021-10-07'
      group by sku_id) t2
     on t1.sku_id = t2.sku_id;


//37、

/*
 用户等级：
忠实用户：近7天活跃且非新用户
新晋用户：近7天新增
沉睡用户：近7天未活跃但是在7天前活跃
流失用户：近30天未活跃但是在30天前活跃
假设今天是数据中所有日期的最大值，从用户登录明细表中的用户登录时间给各用户分级，求出各等级用户的人数
 */


select count(`if`(date_sub('2021-10-09', 30) >= last_login, 1, null))                                            `流失用户`,
       count(`if`(date_sub('2021-10-09', 7) >= last_login and date_sub('2021-10-09', 30) < last_login, 1,
                  null))                                                                                         `沉睡用户`,
       count(`if`(date_sub('2021-10-09', 7) < first_login, 1, null))                                             `新晋用户`,
       count(`if`(date_sub('2021-10-09', 7) < last_login and date_sub('2021-10-09', 7) >= first_login, 1, null)) `忠实用户`
from (select first_login,
             last_login
      from (select user_id,
                   min(date_format(login_ts, 'yyyy-MM-dd')) first_login,
                   max(date_format(login_ts, 'yyyy-MM-dd')) last_login
            from user_login_detail
            group by user_id) t1) t2;


//实现
select level,
       count(level) cn
from (select case
                 when date_sub('2021-10-09', 30) > last_login then '流失用户'
                 when date_sub('2021-10-09', 7) > last_login then '沉睡用户'
                 when date_sub('2021-10-09', 7) <= first_login then '新增用户'
                 when date_sub('2021-10-09', 7) <= last_login then '忠实用户'
                 end as level
      from (select min(date_format(login_ts, 'yyyy-MM-dd')) first_login,
                   max(date_format(login_ts, 'yyyy-MM-dd')) last_login
            from user_login_detail
            group by user_id) t1) t2
group by t2.level;



//38、

/*
    用户每天签到可以领1金币，并可以累计签到天数，连续签到的第3、7天分别可以额外领2和6金币。
每连续签到7天重新累积签到天数。
从用户登录明细表中求出每个用户金币总数，并按照金币总数倒序排序
 */

select
    user_id,
    sum(cn) sum_coin_cn
from
(select
    user_id,
    `if`(count(user_id)%7>=3,count(user_id)%7+2,count(user_id)%7)+cast(count(user_id)/7 as int)*6 cn
from (select user_id,
             date_sub(login_date, rn) flag_group
      from (select user_id,
                   date_format(login_ts, 'yyyy-MM-dd')                                                   login_date,
                   row_number() over (partition by user_id order by date_format(login_ts, 'yyyy-MM-dd')) rn
            from user_login_detail
            group by user_id, date_format(login_ts, 'yyyy-MM-dd')) t1) t2
group by user_id, flag_group) t3
group by user_id;


//39、

-- 动销率定义为品类商品中一段时间内有销量的商品占当前已上架总商品数的比例（有销量的商品/已上架总商品数）。
-- 滞销率定义为品类商品中一段时间内没有销量的商品占当前已上架总商品数的比例。（没有销量的商品 / 已上架总商品数）。
-- 只要当天任一店铺有任何商品的销量就输出该天的结果
-- 从订单明细表（order_detail）和商品信息表（sku_info）表中求出国庆7天每天每个品类的商品的动销率和滞销率


select
    category_id,
    sum(`if`(create_date='2021-10-01',num,0.00)) first_sale_rate,
    1.00-sum(`if`(create_date='2021-10-01',num,0.00)) first_unsale_rate,
    sum(`if`(create_date='2021-10-02',num,0.00)) second_sale_rate,
    1.00-sum(`if`(create_date='2021-10-02',num,0.00)) second_unsale_rate,
    sum(`if`(create_date='2021-10-03',num,0.00)) third_sale_rate,
    1.00-sum(`if`(create_date='2021-10-03',num,0.00)) third_unsale_rate,
    sum(`if`(create_date='2021-10-04',num,0.00)) fourth_sale_rate,
    1.00-sum(`if`(create_date='2021-10-04',num,0.00)) fourth_unsale_rate,
    sum(`if`(create_date='2021-10-05',num,0.00)) fifth_sale_rate,
    1.00-sum(`if`(create_date='2021-10-05',num,0.00)) fifth_unsale_rate,
    sum(`if`(create_date='2021-10-06',num,0.00)) sixth_sale_rate,
    1.00-sum(`if`(create_date='2021-10-06',num,0.00)) sixth_unsale_rate,
    sum(`if`(create_date='2021-10-07',num,0.00)) seventh_sale_rate,
    1.00-sum(`if`(create_date='2021-10-07',num,0.00)) seventh_unsale_rate
from
(select
    category_id,
    create_date,
    cast(count(category_id)/collect_set(cn)[0] as decimal(16,2)) num
from
(select
    create_date,
    sku_id
from order_detail
where create_date between '2021-10-01' and '2021-10-07'
group by create_date,sku_id) t1
join
(select sku_id,
       category_id,
       count(category_id) over (partition by category_id) cn
from sku_info) t2
on t1.sku_id=t2.sku_id
group by category_id,create_date) t3
group by category_id;


//40、根据用户登录明细表（user_login_detail），求出平台同时在线最多的人数。


select
    max(count) cn
from
(select
    sum(flag) over (order by ts) count
from
(select
    login_ts ts,
    1 flag
from user_login_detail
union
select
    logout_ts ts,
    -1 flag
from user_login_detail) t1 ) t2;


//41、现有各直播间的用户访问记录表（live_events）如下，表中每行数据表达的信息为，一个用户何时进入了一个直播间，又在何时离开了该直播间。

//现要求统计各直播间最大同时在线人数，期望结果如下：

select
    live_id,
    max(num) max_user_count
from
(select
    live_id,
    sum(flag) over (partition by live_id order by datetime) num
from
(select
    user_id,
    live_id,
    in_datetime datetime,
    1 flag
from live_events
union
select
    user_id,
    live_id,
    out_datetime datetime,
    -1 flag
from live_events) t1 ) t2
group by live_id;


//42、现有页面浏览记录表（page_view_events）如下，表中有每个用户的每次页面访问记录。

/*
    规定若同一用户的相邻两次访问记录时间间隔小于60s，则认为两次浏览记录属于同一会话。
    现有如下需求，为属于同一会话的访问记录增加一个相同的会话id字段，会话id格式为"user_id-number"，
    其中number从1开始，用于区分同一用户的不同会话，期望结果如下：
 */

select
    user_id,
    page_id,
    view_timestamp,
    concat(user_id,'-',sum(flag) over (partition by user_id order by view_timestamp))  session_id
from
(select
    user_id,
    page_id,
    view_timestamp,
    `if`((view_timestamp-(lag(view_timestamp,1,0) over (partition by user_id order by view_timestamp asc ))) <60,0,1) flag
from page_view_events) t1;


//43、现有各用户的登录记录表（login_events）如下，表中每行数据表达的信息是一个用户何时登录了平台。

/*
 现要求统计各用户最长的连续登录天数，间断一天也算作连续，例如：一个用户在1,3,5,6登录，则视为连续6天登录。
 */

select
    user_id,
    max(max_day_count) max_day_count
from
(select
    user_id,
    datediff(max(datetime),min(datetime))+1 max_day_count
from
(select
    user_id,
    datetime,
    sum(flag) over (partition by user_id order by datetime) flag
from
(select
    user_id,
    date_format(login_datetime,'yyyy-MM-dd') datetime,
    `if`(datediff(date_format(login_datetime,'yyyy-MM-dd'),(lag(date_format(login_datetime,'yyyy-MM-dd'),1,'0000-00-00') over (partition by user_id order by date_format(login_datetime,'yyyy-MM-dd'))))<=2,0,1) flag
from login_events
group by user_id, date_format(login_datetime,'yyyy-MM-dd'))t1) t2
group by user_id,flag) t3
group by user_id;


//44、现有各品牌优惠周期表（promotion_info）如下，其记录了每个品牌的每个优惠活动的周期，其中同一品牌的不同优惠活动的周期可能会有交叉。

/*

 现要求统计每个品牌的优惠总天数，若某个品牌在同一天有多个优惠活动，则只按一天计算。期望结果如下：
 */


select
    brand,
    sum(promotion_day_count) promotion_day_count
from
(select
    brand,
    datediff(max(all_date),min(all_date))+1 promotion_day_count
from
(select
    brand,
    all_date,
    sum(flag) over (partition by brand order by all_date) new_flag
from
(select
    brand,
    all_date,
    `if`(lag(flag,1,1) over (partition by brand order by all_date)=1,1,0) flag
from
(select
    brand,
    all_date,
    `if`(flag_new=0,1,0) flag
from
(select
    brand,
    all_date,
    flag,
    sum(flag) over (partition by brand order by all_date asc ,flag desc ) flag_new
from
(select
    brand,
    start_date all_date,
    1 flag
from promotion_info
union
select
    brand,
    end_date all_date,
    -1 flag
from promotion_info) t1 ) t2) t3 ) t4 ) t5
group by brand,new_flag) t6
group by brand;


//46、复购率指用户在一段时间内对某商品的重复购买比例，复购率越大，则反映出消费者对品牌的忠诚度就越高，也叫回头率
SELECT
    product_id,
    cast(count(if(num>1,1,null))/count(user_id) as DECIMAL(16,2)) cpr
FROM
    (SELECT
         product_id,
         user_id,
         count(user_id) num
     from
         (SELECT
              *
          from
              (SELECT
                   *,
                   max(order_date) over() recent_day
               from
                   order_detail) t1
          WHERE datediff(recent_day,order_date)<90) t2
     GROUP BY product_id,user_id) t3
GROUP BY product_id;

//47、出勤率指用户看直播时间超过40分钟，求出每个课程的出勤率（结果保留两位小数）。

SELECT
    t1.course_id,
    CAST(one/two as DECIMAL(16,2)) adr
from
    (SELECT
         course_id,
         count(flag) one
     from
         (SELECT
              course_id,
              user_id,
              if(cast(sum(unix_timestamp(login_out)-unix_timestamp(login_in))/60 as int)>40,1,null) flag
          from user_login
          GROUP by course_id,user_id) t1
     GROUP by course_id) t1
        join (
        SELECT course_id,size(user_id) two from course_apply
    ) t2 on t1.course_id=t2.course_id;

//48、

/*

    统计周一到周五各时段的叫车量、平均等待接单时间和平均调度时间。
    全部以event_time-开始打车时间为时段划分依据，平均等待接单时间和平均调度时间均保留2位小数，
    平均调度时间仅计算完成了的订单，结果按叫车量升序排序。
 */

SELECT
    period,
    count(period) get_car_num,
    cast(sum(wait_time)/count(period)/60 as DECIMAL(16,2)) wait_time,
    cast(sum(dispatch_time)/count(period)/60 as DECIMAL(16,2)) dispatch_time
FROM
    (SELECT
         CASE
             WHEN HOUR(event_time)<7 OR HOUR(event_time)>=20 then '休息时间'
             WHEN HOUR(event_time)<9 then '早高峰'
             WHEN HOUR(event_time)<17 then '工作时间'
             WHEN HOUR(event_time)<20 then '晚高峰'
             end as period,
         unix_timestamp(t2.order_time)-unix_timestamp(t1.event_time) wait_time,
         unix_timestamp(t2.start_time)-unix_timestamp(t2.order_time) dispatch_time
     from get_car_record t1
              LEFT JOIN
          get_car_order t2
          on t1.order_id=t2.order_id) t3
GROUP BY period

//49、拿到所有球队比赛的组合 每个队只比一次

SELECT
    t3.a team_name_2,
    t3.b team_name_1
from
    (SELECT
         t1.team_name a,
         t2.team_name b,
         row_number() over (partition by t1.flag+t2.flag order by t2.team_name) rn
     from
         (SELECT
              team_name,
              CASE team_name WHEN  '湖人' THEN 1
                             WHEN  '骑士' THEN 10
                             WHEN  '灰熊' THEN 100
                             WHEN  '勇士' THEN 1000
                  END as flag
          from team) t1
             JOIN
         (SELECT
              team_name,
              CASE team_name WHEN  '湖人' THEN 1
                             WHEN  '骑士' THEN 10
                             WHEN  '灰熊' THEN 100
                             WHEN  '勇士' THEN 1000
                  END as flag
          from team) t2
         on t1.team_name!=t2.team_name) t3
where rn=1;


//49、找出近一个月发布的视频中热度最高的top3视频



//50、统计2020年每个月实际在职员工数量(只统计2020-03-31之前)，如果1个月在职天数只有1天，数量计算方式：1/当月天数。

//如果一个月只有一天的话，只算30分之1个人

SELECT
    mth,
    sum(ps) ps
from
    (SELECT
         MONTH(dt) mth,
         id,
         cast(count(flag)/count(id) as DECIMAL(16,2)) ps
     from
         (SELECT
              id,
              dt,
              if(dt>=en_dt and dt<=nvl(le_dt,'9999-12-31'),1,null) flag
          FROM
              emp t1 FULL join cal t2) t3
     GROUP BY MONTH(dt),id) t4
GROUP BY mth