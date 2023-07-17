//11.1 流量主题

//11.1.1 各渠道流量统计

DROP TABLE IF EXISTS ads_traffic_stats_by_channel;
CREATE EXTERNAL TABLE ads_traffic_stats_by_channel
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `channel`          STRING COMMENT '渠道',
    `uv_count`         BIGINT COMMENT '访客人数',
    `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒',
    `avg_page_count`   BIGINT COMMENT '会话平均浏览页面数',
    `sv_count`         BIGINT COMMENT '会话数',
    `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
) COMMENT '各渠道流量统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_traffic_stats_by_channel/';


//每日装载


insert into table ads_traffic_stats_by_channel
select * from ads_traffic_stats_by_channel
union
select '2022-06-04' dt,
       recent_days,
       channel,
       count(distinct (mid_id)),
       cast(avg(during_time_1d)/1000 as bigint),
       cast(avg(page_count_1d) as bigint),
       count(session_id),
       cast(count(`if`(page_count_1d = 1, 1, null)) / count(session_id) as decimal(16, 2))
from dws_traffic_session_page_view_1d
         lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt > date_sub('2022-06-04', recent_days)
group by recent_days, channel;


//11.1.2 路径分析


DROP TABLE IF EXISTS ads_page_path;
CREATE EXTERNAL TABLE ads_page_path
(
    `dt`         STRING COMMENT '统计日期',
    `source`     STRING COMMENT '跳转起始页面ID',
    `target`     STRING COMMENT '跳转终到页面ID',
    `path_count` BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_page_path/';


//数据装载
insert into table ads_page_path
select '2022-06-04' dt,
       source,
       target,
       count(*)     path_count
from (select sid,
             source,
             lead(source, 1, null) over (partition by sid order by sid,date_id) target
      from (select common.sid,
                   page.page_id                                                        source,
                   date_format(from_utc_timestamp(ts, 'GTM+8'), 'yyyy-MM-dd HH:mm:ss') date_id,
                   row_number() over (partition by common.sid,ts)                      rn,
                   ts
            from ods_log_inc
            where page is not null
              and dt = '2022-06-04') t1
      where rn = 1) t2
where t2.target is not null
group by source, target;

//每日装载

insert overwrite table ads_page_path
select * from ads_page_path
union
select
    '2022-06-10' dt,
    source,
    nvl(target,'null'),
    count(*) path_count
from
    (
        select
            concat('step-',rn,':',page_id) source,
            concat('step-',rn+1,':',next_page_id) target
        from
            (
                select
                    page_id,
                    lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
                    row_number() over (partition by session_id order by view_time) rn
                from dwd_traffic_page_view_inc
                where dt='2022-06-10'
            )t1
    )t2
group by source,target;


set spark.executor.memory=2g;
set hive.auto.convert.join=false;
set mapred.map.tasks.speculative.execution=true;
set mapred.reduce.tasks.speculative.execution=true;



//11.2 用户主题

//11.2.1 用户变动统计

/*
    四天不登录不登录,就算流失
 */

DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE ads_user_change
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_change/';



insert overwrite table ads_user_change
select *
from ads_user_change
union;
select
    bk.dt,
    user_churn_count,
    user_back_count
from
    (
        select '2022-06-09' dt,
               count(*) user_churn_count
        from dws_user_user_login_td
        where dt='2022-06-09'
        and datediff(dt,login_date_last)=4
    ) cn
    join
(select
    '2022-06-09' dt,
    count(*) user_back_count
from
(select user_id,
        login_date_last login_date_previous
from dws_user_user_login_td
where date_add(dt, 1) = '2022-06-09'
  and datediff(dt, login_date_last) > 3) t1
               join (select user_id,
                            login_date_last
                          from dws_user_user_login_td
                          where dt = '2022-06-09'
                            and dt=login_date_last
                          ) t2 on t2.user_id = t1.user_id
where datediff(login_date_last,login_date_previous)>7
) bk
on bk.dt=cn.dt;




//案例
select * from ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
    (
        select
            '2022-06-09' dt,
            count(*) user_churn_count
        from dws_user_user_login_td
        where dt='2022-06-09'
          and login_date_last=date_add('2022-06-09',-7)
    )churn
        join
    (
        select
            '2022-06-09' dt,
            count(*) user_back_count
        from
            (
                select
                    user_id,
                    login_date_last
                from dws_user_user_login_td
                where dt='2022-06-09'
                  and login_date_last = '2022-06-09'
            )t1
                join
            (
                select
                    user_id,
                    login_date_last login_date_previous
                from dws_user_user_login_td
                where dt=date_add('2022-06-09',-1)
            )t2
            on t1.user_id=t2.user_id
        where datediff(login_date_last,login_date_previous)>=8
    )back
    on churn.dt=back.dt;





//11.2 用户主题

//11.2.2 用户留存率


DROP TABLE IF EXISTS ads_user_retention;
CREATE EXTERNAL TABLE ads_user_retention
(
    `dt`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_retention/';

/*
    1、先统计昨天所有的用户第一个和最后一次登录日期,然后按注册日期分组
    2、留存用户即最后登录日期为昨天
 */

;

insert overwrite table ads_user_retention
select *
from ads_user_retention
union
select '2022-06-09'                                                                           dt,
       login_date_first                                                                       create_date,
       datediff('2022-06-09', login_date_first)                                               retention_day,
       sum(`if`(login_date_last = '2022-06-09', 1, 0))                                        retention_count,
       count(*)                                                                               new_user_count,
       cast(sum(if(login_date_last = '2022-06-09', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (select user_id,
             login_date_last,
             login_date_first
      from dws_user_user_login_td
      where dt = '2022-06-09'
        and login_date_first >= date_add('2022-06-09', -7)
        and login_date_first < '2022-06-09') t1
group by login_date_first;


//11.2.3 用户新增活跃统计

DROP TABLE IF EXISTS ads_user_stats;
CREATE EXTERNAL TABLE ads_user_stats
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) COMMENT '用户新增活跃统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_stats/';


//
insert overwrite table ads_user_stats
select *
from ads_user_stats
union
select '2022-06-09'                                                  dt,
       recent_days,
       sum(`if`(login_date_first > date_sub(dt, recent_days), 1, 0)) new_user_count,
       sum(`if`(login_date_last > date_sub(dt, recent_days), 1, 0))  active_user_count
from dws_user_user_login_td
         lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '2022-06-09'
group by recent_days;


//11.2.4 用户行为漏斗分析

DROP TABLE IF EXISTS ads_user_action;
CREATE EXTERNAL TABLE ads_user_action
(
    `dt`                STRING COMMENT '统计日期',
    `home_count`        BIGINT COMMENT '浏览首页人数',
    `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        BIGINT COMMENT '加购人数',
    `order_count`       BIGINT COMMENT '下单人数',
    `payment_count`     BIGINT COMMENT '支付人数'
) COMMENT '用户行为漏斗分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_action/';


//数据装载

insert overwrite table ads_user_action
select *
from ads_user_action
union;
select '2022-06-10' dt,
       *
from (select count(`if`(page_id = 'home', 1, null))        home_count,
             count(`if`(page_id = 'good_detail', 1, null)) good_detail_count
      from dws_traffic_page_visitor_page_view_1d
      where dt <= '2022-06-10'
        and dt > date_sub('2022-06-10', 1)) t1
         full join (select count(user_id) cart_count
                    from dws_trade_user_cart_add_1d
                    where dt <= '2022-06-10'
                      and dt > date_sub('2022-06-10', 1)) t2
         full join (select count(user_id) order_count
                    from dws_trade_user_order_1d
                    where dt <= '2022-06-10'
                      and dt > date_sub('2022-06-10', 1)) t3
         full join (select count(user_id) payment_count
                    from dws_trade_user_payment_1d
                    where dt <= '2022-06-10'
                      and dt > date_sub('2022-06-10', 1)) t4;


select t1.user_id,
       t1.order_count_1d,
       t2.payment_count_1d
from dws_trade_user_order_1d t1
         left join dws_trade_user_payment_1d t2
                   on t1.user_id = t2.user_id
where t1.dt = '2022-06-08'
  and t2.dt = '2022-06-08';



select *
from dwd_trade_order_detail_inc t1
         left join dwd_trade_pay_detail_suc_inc t2
                   on t1.order_id = t2.order_id
where t1.order_id = '38227'
  and t1.dt = '2022-06-08';


//11.2.5 新增下单用户统计

DROP TABLE IF EXISTS ads_new_order_user_stats;
CREATE EXTERNAL TABLE ads_new_order_user_stats
(
    `dt`                   STRING COMMENT '统计日期',
    `recent_days`          BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count` BIGINT COMMENT '新增下单人数'
) COMMENT '新增下单用户统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_new_order_user_stats/';


//数据装载

select recent_days,
       count(*)
from dws_trade_user_order_td
         lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt <= '2022-06-08'
  and dt > date_sub('2022-06-08', recent_days)
group by recent_days;


insert overwrite table ads_new_order_user_stats
select *
from ads_new_order_user_stats
union
select '2022-06-08',
       recent_days,
       count(*)
from dws_trade_user_order_td
         lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '2022-06-08'
  and order_date_first > date_sub('2022-06-08', recent_days)
group by recent_days;


//11.2.6 最近7日内连续3日下单用户数

DROP TABLE IF EXISTS ads_order_continuously_user_count;
CREATE EXTERNAL TABLE ads_order_continuously_user_count
(
    `dt`                            STRING COMMENT '统计日期',
    `recent_days`                   BIGINT COMMENT '最近天数,7:最近7天',
    `order_continuously_user_count` BIGINT COMMENT '连续3日下单用户数'
) COMMENT '最近7日内连续3日下单用户数统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_continuously_user_count/';


//数据装载
select '2022-06-08' dt,
       7,
       count(*)     order_continuously_user_count
from (select user_id,
             count(*) num
      from (select user_id,
                   dt,
                   row_number() over (partition by user_id order by dt) rn
            from dws_trade_user_order_1d
                     lateral view explode(array(7)) tmp as recent_days
            where dt <= '2022-06-08'
              and dt > date_sub('2022-06-08', recent_days)) t1
      group by user_id, date_sub(dt, rn)) t2
where t2.num >= 3;



insert overwrite table ads_order_continuously_user_count
select *
from ads_order_continuously_user_count
union
select '2022-06-08' dt,
       recent_days,
       count(*)     order_continuously_user_count
from (select recent_days,
             user_id,
             count(*) num
      from (select recent_days,
                   user_id,
                   dt,
                   row_number() over (partition by recent_days,user_id order by dt) rn
            from dws_trade_user_order_1d
                     lateral view explode(array(7)) tmp as recent_days
            where dt <= '2022-06-08'
              and dt > date_sub('2022-06-08', recent_days)
            group by recent_days, user_id, dt) t1
      group by recent_days, user_id, date_sub(dt, rn)) t2
where t2.num >= 3
group by recent_days;


//11.3 商品主题

//11.3.1 最近30日各品牌复购率
DROP TABLE IF EXISTS ads_repeat_purchase_by_tm;
CREATE EXTERNAL TABLE ads_repeat_purchase_by_tm
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,30:最近30天',
    `tm_id`             STRING COMMENT '品牌ID',
    `tm_name`           STRING COMMENT '品牌名称',
    `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率'
) COMMENT '最近30日各品牌复购率统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_repeat_purchase_by_tm/';


//数据装载


insert overwrite table ads_repeat_purchase_by_tm
select *
from ads_repeat_purchase_by_tm
union
select '2022-06-09'                                                                                 dt,
       30                                                                                           recent_days,
       tm_id,
       tm_name,
       cast(count(`if`(num >= 2, 1, null)) / count(`if`(num = 1, 1, null)) * 100 as decimal(16, 2)) order_repeat_rate
from (select tm_id,
             tm_name,
             user_id,
             sum(order_count_30d) num
      from dws_trade_user_sku_order_nd
      where dt = '2022-06-09'
      group by tm_id, tm_name, user_id) t1
group by tm_id, tm_name;


//11.3.2 各品牌商品下单统计

DROP TABLE IF EXISTS ads_order_stats_by_tm;
CREATE EXTERNAL TABLE ads_order_stats_by_tm
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`            STRING COMMENT '品牌ID',
    `tm_name`          STRING COMMENT '品牌名称',
    `order_count`      BIGINT COMMENT '下单数',
    `order_user_count` BIGINT COMMENT '下单人数'
) COMMENT '各品牌商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_stats_by_tm/';


//数据装载

insert overwrite table ads_order_stats_by_tm
select *
from ads_order_stats_by_tm
union
select '2022-06-08',
       recent_days,
       tm_id,
       tm_name,
       sum(order_count_1d),
       count(distinct user_id)
from dws_trade_user_sku_order_1d
         lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt <= '2022-06-08'
  and dt > date_sub('2022-06-08', recent_days)
group by recent_days, tm_id, tm_name;


//11.3.3 各品类商品下单统计

DROP TABLE IF EXISTS ads_order_stats_by_cate;
CREATE EXTERNAL TABLE ads_order_stats_by_cate
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category1_id`     STRING COMMENT '一级品类ID',
    `category1_name`   STRING COMMENT '一级品类名称',
    `category2_id`     STRING COMMENT '二级品类ID',
    `category2_name`   STRING COMMENT '二级品类名称',
    `category3_id`     STRING COMMENT '三级品类ID',
    `category3_name`   STRING COMMENT '三级品类名称',
    `order_count`      BIGINT COMMENT '下单数',
    `order_user_count` BIGINT COMMENT '下单人数'
) COMMENT '各品类商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_stats_by_cate/';


//数据装载


insert overwrite table ads_order_stats_by_cate
select *
from ads_order_stats_by_cate
union
select '2022-06-08',
       recent_days,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       sum(order_count_1d),
       count(distinct user_id)
from dws_trade_user_sku_order_1d
         lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt <= '2022-06-08'
  and dt > date_sub('2022-06-08', recent_days)
group by recent_days, category1_id, category1_name,
         category2_id, category2_name,
         category3_id, category3_name;


//11.3.4 各品类商品购物车存量Top3

DROP TABLE IF EXISTS ads_sku_cart_num_top3_by_cate;
CREATE EXTERNAL TABLE ads_sku_cart_num_top3_by_cate
(
    `dt`             STRING COMMENT '统计日期',
    `category1_id`   STRING COMMENT '一级品类ID',
    `category1_name` STRING COMMENT '一级品类名称',
    `category2_id`   STRING COMMENT '二级品类ID',
    `category2_name` STRING COMMENT '二级品类名称',
    `category3_id`   STRING COMMENT '三级品类ID',
    `category3_name` STRING COMMENT '三级品类名称',
    `sku_id`         STRING COMMENT 'SKU_ID',
    `sku_name`       STRING COMMENT 'SKU名称',
    `cart_num`       BIGINT COMMENT '购物车中商品数量',
    `rk`             BIGINT COMMENT '排名'
) COMMENT '各品类商品购物车存量Top3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate/';


//每日装载

insert overwrite table ads_sku_cart_num_top3_by_cate
select *
from ads_sku_cart_num_top3_by_cate
union
select *
from (select '2022-06-09',
             category1_id,
             category1_name,
             category2_id,
             category2_name,
             category3_id,
             category3_name,
             sku_id,
             t1.sku_name,
             sum(sku_num)                                                 num,
             dense_rank() over (partition by category1_id, category1_name,
                 category2_id, category2_name,
                 category3_id, category3_name order by sum(sku_num) desc) rn
      from dwd_trade_cart_full t1
               join dim_sku_full t2
                    on t1.sku_id = t2.id
      where t2.dt = '2022-06-09'
        and t1.dt = '2022-06-09'
      group by category1_id, category1_name,
               category2_id, category2_name,
               category3_id, category3_name, sku_id, t1.sku_name) t3
where rn <= 3;


//  11.3.5 各品牌商品收藏次数Top3


DROP TABLE IF EXISTS ads_sku_favor_count_top3_by_tm;
CREATE EXTERNAL TABLE ads_sku_favor_count_top3_by_tm
(
    `dt`          STRING COMMENT '统计日期',
    `tm_id`       STRING COMMENT '品牌ID',
    `tm_name`     STRING COMMENT '品牌名称',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `sku_name`    STRING COMMENT 'SKU名称',
    `favor_count` BIGINT COMMENT '被收藏次数',
    `rk`          BIGINT COMMENT '排名'
) COMMENT '各品牌商品收藏次数Top3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_sku_favor_count_top3_by_tm/';


//每日装载

insert overwrite table ads_sku_favor_count_top3_by_tm
select *
from ads_sku_favor_count_top3_by_tm
union
select '2022-06-08',
       tm_id,
       tm_name,
       sku_id,
       sku_name,
       favor_add_count_1d,
       dense_rank() over (partition by tm_id,tm_name order by favor_add_count_1d desc ) dr
from dws_interaction_sku_favor_add_1d
where dt = '2022-06-08';



//11.4 交易主题


//11.4.1 下单到支付时间间隔平均值


DROP TABLE IF EXISTS ads_order_to_pay_interval_avg;
CREATE EXTERNAL TABLE ads_order_to_pay_interval_avg
(
    `dt`                        STRING COMMENT '统计日期',
    `order_to_pay_interval_avg` BIGINT COMMENT '下单到支付时间间隔平均值,单位为秒'
) COMMENT '下单到支付时间间隔平均值统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_to_pay_interval_avg/';


//每日装载
insert overwrite table ads_order_to_pay_interval_avg
select *
from ads_order_to_pay_interval_avg
union
select '2022-06-09',
       cast(sum(unix_timestamp(payment_time, 'yyyy-MM-dd hh:mm:ss') -
                unix_timestamp(order_time, 'yyyy-MM-dd hh:mm:ss')) / count(order_id) as bigint)
from dwd_trade_trade_flow_acc
where payment_date_id = '2022-06-09';


//11.4.2 各省份交易统计

DROP TABLE IF EXISTS ads_order_by_province;
CREATE EXTERNAL TABLE ads_order_by_province
(
    `dt`                 STRING COMMENT '统计日期',
    `recent_days`        BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`        STRING COMMENT '省份ID',
    `province_name`      STRING COMMENT '省份名称',
    `area_code`          STRING COMMENT '地区编码',
    `iso_code`           STRING COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_code_3166_2`    STRING COMMENT '新版国际标准地区编码，供可视化使用',
    `order_count`        BIGINT COMMENT '订单数',
    `order_total_amount` DECIMAL(16, 2) COMMENT '订单金额'
) COMMENT '各省份交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_by_province/';


//数据装载

insert overwrite table ads_order_by_province
select *
from ads_order_by_province
union
select '2022-06-08',
       recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       sum(order_count_1d),
       sum(order_total_amount_1d)
from dws_trade_province_order_1d
         lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt <= '2022-06-08'
  and dt > date_sub('2022-06-08', recent_days)
group by recent_days, province_id, province_name, area_code, iso_code, iso_3166_2;




//11.5 优惠券主题

//优惠券使用统计

DROP TABLE IF EXISTS ads_coupon_stats;
CREATE EXTERNAL TABLE ads_coupon_stats
(
    `dt`              STRING COMMENT '统计日期',
    `coupon_id`       STRING COMMENT '优惠券ID',
    `coupon_name`     STRING COMMENT '优惠券名称',
    `used_count`      BIGINT COMMENT '使用次数',
    `used_user_count` BIGINT COMMENT '使用人数'
) COMMENT '优惠券使用统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_coupon_stats/';



//每日装载

insert overwrite table ads_coupon_stats
select *
from ads_coupon_stats
union
select
    '2022-06-08',
    coupon_id,
    coupon_name,
    sum(used_count_1d),
    count(distinct user_id)
from dws_tool_user_coupon_coupon_used_1d
where dt='2022-06-08'
group by coupon_id,coupon_name;


