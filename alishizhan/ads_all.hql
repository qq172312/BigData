------------------------1.1.1各来源流量统计-------------------------
DROP TABLE IF EXISTS ads_traffic_stats_by_source;
CREATE EXTERNAL TABLE ads_traffic_stats_by_source
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source`          STRING COMMENT '来源',
    `uv_count`         BIGINT COMMENT '访客人数',
    `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒',
    `avg_page_count`   BIGINT COMMENT '会话平均浏览页面数',
    `sv_count`         BIGINT COMMENT '会话数',
    `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
) COMMENT '各来源流量统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_traffic_stats_by_source/';

--代码实现
insert overwrite table ads_traffic_stats_by_source
select * from ads_traffic_stats_by_source
union
select '2022-06-08',
       recent_days,
       source_site,
       cast(count(distinct mid_id) as bigint) ,
       cast(avg(during_time_1d)/1000 as bigint),
       cast(avg(page_count_1d) as bigint),
       cast(count(*) as bigint),
       cast(sum(if(page_count_1d = 1,1,0)) / count(*) as decimal(16,2))
from dws_traffic_session_page_view_1d lateral view explode (array(1,7,30)) tmp as recent_days
where dt >= date_sub('2022-06-08',recent_days - 1)
group by recent_days,source_site;


-------------------------1.1.2路径分析----------------------------------
DROP TABLE IF EXISTS ads_page_path;
CREATE EXTERNAL TABLE ads_page_path
(
    `dt`          STRING COMMENT '统计日期',
    `source`      STRING COMMENT '跳转起始页面ID',
    `target`      STRING COMMENT '跳转终到页面ID',
    `path_count`  BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_page_path/';



insert overwrite table ads_page_path
select * from ads_page_path
union
select '2022-06-08' dt,
       source,
       nvl(target,'null'),
       count(*) path_count
from (select concat('step-',rn,':',page_id) source,
             concat('step-',rn+1,':',next_page_id) target
      from (select page_id,
                   lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
                   row_number() over (partition by session_id order by view_time) rn
            from dwd_traffic_page_view_inc
            where dt = '2022-06-08') t1)t2
group by source,target;

--------------------------1.1.3各来源下单统计-------------------------------
DROP TABLE IF EXISTS ads_order_amount_by_source;
CREATE EXTERNAL TABLE ads_order_amount_by_source
(
    `dt`          STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source_id`      STRING COMMENT '来源id',
    `source_site`      STRING COMMENT '来源名称',
    `order_amount`      decimal(16,2) COMMENT '销售额',
    `conversion_rate`  decimal(16,2) COMMENT '转化率'
) COMMENT '页面浏览路径分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_order_amount_by_source/';



insert overwrite table ads_order_amount_by_source
select '2022-06-08',
       1,
       '1',
       '222',
       222,
       22.22;


insert overwrite table ads_order_amount_by_source
select '2022-06-08',
       1,
       '1',
       '222',
       222,
       22.22
from ads_order_amount_by_source
union
select '2022-06-08',
       days,
       t.source_id,
       source_name,
       order_amount,
       cast((order_num / sour_num) as decimal(16,2) )
from (select
          days,
          source_id,
          source_name,
          sum(final_amount) order_amount,
          count(distinct user_id) order_num
      from dwd_trade_order_detail_inc lateral view explode(array(1,7,30)) tmp as days
      where dt >= date_sub('2022-06-08',days - 1) and dt <='2022-06-08'
      group by days,source_id,source_name) t
         left join (
    select rec,
           source_id,
           count(distinct mid_id) sour_num
    from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as rec
    where dt >= date_sub('2022-06-08',rec - 1) and dt <= '2022-06-08'
    group by source_id,rec) t1
                   on t.source_id = t1.source_id and t.days = t1.rec
where t.source_id is not null;


--1.2.1用户变动统计
DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE ads_user_change
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_user_change/';



insert overwrite table ads_user_change
select * from ads_user_change
union
select t3.dt,
       user_churn_count,
       user_back_count
from (
         select "2022-06-08" dt ,
                count(*) user_churn_count
         from dws_user_user_login_td
         where dt="2022-06-08" and login_date_last = date_sub("2022-06-08",7)
     )t3
         join (
    select "2022-06-08" dt,
           count(*) user_back_count
    from (
             select user_id,
                    "2022-06-08" dt,
                    login_date_last
             from dws_user_user_login_td
             where dt = "2022-06-08" and login_date_last="2022-06-08"
         )t1
             left join (
        select user_id,
               login_date_last
        from dws_user_user_login_td
        where dt = date_sub("2022-06-08",1)
    )t2 on t1.user_id=t2.user_id
    where datediff(t1.login_date_last,t2.login_date_last) >=8
)t4 on t3.dt=t4.dt;



--1.2.2用户留存率
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
    LOCATION '/warehouse/edu/ads/ads_user_retention/';

insert overwrite table ads_user_retention
select * from ads_user_retention
union
select '2022-06-08' dt,
       login_date_first create_date,
       datediff('2022-06-08', login_date_first) retention_day,
       sum(if(login_date_last = '2022-06-08', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '2022-06-08', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                login_date_last,
                login_date_first
         from dws_user_user_login_td
         where dt = '2022-06-08'
           and login_date_first >= date_add('2022-06-08', -7)
           and login_date_first < '2022-06-08'
     ) t1
group by login_date_first;

--1.2.3用户新增活跃统计
DROP TABLE IF EXISTS ads_user_stats;
CREATE EXTERNAL TABLE ads_user_stats
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) COMMENT '用户新增活跃统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_user_stats/';

insert overwrite table ads_user_stats
select * from ads_user_stats
union
select "2022-06-08" dt ,
       recent_days,
       sum(`if`(login_date_first>=date_sub("2022-06-08",recent_days-1),1,0)) new_user_count,
       sum(`if`(login_date_last>=date_sub("2022-06-08",recent_days-1),1,0)) active_user_count
from dws_user_user_login_td lateral view explode(`array`(1,7,30)) tmp as recent_days
where dt = "2022-06-08"
group by dt,recent_days;



--1.2.4用户行为漏斗分析
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
    LOCATION '/warehouse/edu/ads/ads_user_action/';

insert overwrite table ads_user_action
select *
from ads_user_action
union
select
    t1.`dt`                ,--STRING COMMENT '统计日期',
    `home_count`        ,--BIGINT COMMENT '浏览首页人数',
    `good_detail_count` ,--BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        ,--BIGINT COMMENT '加购人数',
    `order_count`       ,--BIGINT COMMENT '下单人数',
    `payment_count`     --BIGINT COMMENT '支付人数'
from (
         select dt,
                count(`if`(page_id="home",1,null)) home_count,
                count(`if`(page_id="course_detail",1,null)) good_detail_count
         from dws_traffic_page_visitor_page_view_1d
         where dt="2022-06-08"
         group by dt
     )t1
         left join (
    select dt,
           count(*) cart_count
    from dws_trade_user_cart_add_1d
    where dt="2022-06-08"
    group by dt
)t2 on t1.dt=t2.dt
         left join (
    select dt,
           count(*) order_count
    from dws_trade_user_order_1d
    where dt="2022-06-08"
    group by dt
)t3 on t2.dt=t3.dt
         left join (
    select dt,
           count(*) payment_count
    from dws_trade_user_payment_1d
    where dt="2022-06-08"
    group by dt
)t4 on t3.dt=t4.dt;

//1.2.5 新增交易用户统计

DROP TABLE IF EXISTS ads_order_stats_by_add;
CREATE EXTERNAL TABLE ads_order_stats_by_add
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `order_count`                   bigint COMMENT '新增下单人数',
    `pay_count`                 bigint COMMENT '新增支付人数'
) COMMENT '新增交易用户统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_order_stats_by_add/';



///
insert overwrite table ads_order_stats_by_add
select *
from ads_order_stats_by_add
union
select
    '2022-06-08' dt,
    recent_days,
    count(`if`(date_sub('2022-06-08',t1.recent_days)<order_date_first,order_date_first,null)),
    count(`if`(date_sub('2022-06-08',t1.recent_days)<pay_date_first,pay_date_first,null))
from
    (select
         recent_days,
         order_date_first,
         pay_date_first
     from dws_trade_user_order_td
              lateral view explode(array(1,7,30)) tmp as recent_days
     where order_date_first>date_sub('2022-06-08',recent_days) or pay_date_first>date_sub('2022-06-08',recent_days)
    ) t1
group by recent_days;


//1.2.6 各年龄段下单用户数

DROP TABLE IF EXISTS ads_order_stats_by_age;
CREATE EXTERNAL TABLE ads_order_stats_by_age
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `age_cnt`                   STRING COMMENT '年龄段',
    `user_cnt`                 bigint COMMENT '下单人数'
) COMMENT '各年龄段下单用户数'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_order_stats_by_age/';




insert overwrite table ads_order_stats_by_age
select *
from ads_order_stats_by_age
union
select
    '2022-06-08' dt,
    recent_days,
    age_cnt,
    count(distinct user_id)
from
    (select *,
            case when age<-25 then '0-25'
                 when age<=50 then '26-50'
                 else '51-' end as age_cnt
     from dws_trade_user_order_1d
              lateral view explode(array(1,7,30)) tmp as recent_days) t1
where dt>date_sub('2022-06-08',recent_days)
group by recent_days,age_cnt;



//课程主题


//1.3.1 各分类课程交易统计

DROP TABLE IF EXISTS ads_order_stats_by_category;
CREATE EXTERNAL TABLE ads_order_stats_by_category
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category_id`                   STRING COMMENT '分类ID',
    `category_name`                 STRING COMMENT '分类名称',
    `order_count`             BIGINT COMMENT '下单数',
    `order_user_count`        BIGINT COMMENT '下单人数',
    `order_money_count`        BIGINT COMMENT '下单金额'
) COMMENT '各品牌商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_order_stats_by_category/';


//首日
insert overwrite table ads_order_stats_by_category
select * from ads_order_stats_by_category
union
select
    '2022-06-08' dt,
    recent_days,
    category_id,
    category_name,
    order_count,
    order_count_user,
    order_total_amount
from
(select
    1 recent_days,
    category_id,
    category_name,
    sum(order_count_1d) order_count,
    count(distinct (user_id)) order_count_user,
    sum(order_total_amount_1d) order_total_amount
from dws_trade_user_sku_order_1d
where dt='2022-06-08'
group by category_id,category_name
union
select
    recent_days,
    category_id,
    category_name,
    sum(order_count),
    count(distinct (`if`(order_count>0,user_id,null))),
    sum(order_total_amount)
from
(select
    recent_days,
    user_id,
    category_id,
    category_name,
    case recent_days
        when 7 then order_count_7d
when 30 then order_count_30d
end order_count,
    case recent_days
        when 7 then order_total_amount_7d
        when 30 then order_total_amount_30d
        end order_total_amount
from dws_trade_user_sku_order_nd
lateral view explode(array(7,30)) tmp as recent_days
where dt='2022-06-08') t1
group by recent_days,category_id,category_name) t2;




//1.3.2 各科目课程交易统计

DROP TABLE IF EXISTS ads_order_stats_by_subject;
CREATE EXTERNAL TABLE ads_order_stats_by_subject
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `subject_id`                   STRING COMMENT '科目ID',
    `subject_name`                 STRING COMMENT '科目名称',
    `order_count`             BIGINT COMMENT '下单数',
    `order_user_count`        BIGINT COMMENT '下单人数',
    `order_money_count`        BIGINT COMMENT '下单金额'
) COMMENT '各科目商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_order_stats_by_subject/';



//
insert overwrite table ads_order_stats_by_subject
select * from ads_order_stats_by_subject
union
select
    '2022-06-08' dt,
    recent_days,
    subject_id,
    subject_name,
    order_count,
    order_count_user,
    order_total_amount
from
    (select
         1 recent_days,
         subject_id,
         subject_name,
         sum(order_count_1d) order_count,
         count(distinct (user_id)) order_count_user,
         sum(order_total_amount_1d) order_total_amount
     from dws_trade_user_sku_order_1d
     where dt='2022-06-08'
     group by subject_id,subject_name
     union
     select
         recent_days,
         subject_id,
         subject_name,
         sum(order_count),
         count(distinct (`if`(order_count>0,user_id,null))),
         sum(order_total_amount)
     from
         (select
              recent_days,
              user_id,
              subject_id,
              subject_name,
              case recent_days
                  when 7 then order_count_7d
                  when 30 then order_count_30d
                  end order_count,
              case recent_days
                  when 7 then order_total_amount_7d
                  when 30 then order_total_amount_30d
                  end order_total_amount
          from dws_trade_user_sku_order_nd
                   lateral view explode(array(7,30)) tmp as recent_days
          where dt='2022-06-08') t1
     group by recent_days,subject_id,subject_name) t2;




//1.3.3 各课程交易统计

DROP TABLE IF EXISTS ads_order_stats_by_course;
CREATE EXTERNAL TABLE ads_order_stats_by_course
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `course_id`                   STRING COMMENT '课程ID',
    `course_name`                 STRING COMMENT '课程名称',
    `order_count`             BIGINT COMMENT '下单数',
    `order_user_count`        BIGINT COMMENT '下单人数',
    `order_money_count`        BIGINT COMMENT '下单金额'
) COMMENT '各课程交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_order_stats_by_course/';



//
insert overwrite table ads_order_stats_by_course
select * from ads_order_stats_by_course
union
select
    '2022-06-08' dt,
    recent_days,
    course_id,
    course_name,
    order_count,
    order_count_user,
    order_total_amount
from
    (select
         1 recent_days,
         course_id,
         course_name,
         sum(order_count_1d) order_count,
         count(distinct (user_id)) order_count_user,
         sum(order_total_amount_1d) order_total_amount
     from dws_trade_user_sku_order_1d
     where dt='2022-06-08'
     group by course_id,course_name
     union
     select
         recent_days,
         course_id,
         course_name,
         sum(order_count),
         count(distinct (`if`(order_count>0,user_id,null))),
         sum(order_total_amount)
     from
         (select
              recent_days,
              user_id,
              course_id,
              course_name,
              case recent_days
                  when 7 then order_count_7d
                  when 30 then order_count_30d
                  end order_count,
              case recent_days
                  when 7 then order_total_amount_7d
                  when 30 then order_total_amount_30d
                  end order_total_amount
          from dws_trade_user_sku_order_nd
                   lateral view explode(array(7,30)) tmp as recent_days
          where dt='2022-06-08') t1
     group by recent_days,course_id,course_name) t2;



//1.3.4 各课程评价统计

DROP TABLE IF EXISTS ads_review_stats_by_course;
CREATE EXTERNAL TABLE ads_review_stats_by_course
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `course_id`                   STRING COMMENT '课程ID',
    `course_name`                 STRING COMMENT '课程名称',
    `user_avg_score`             BIGINT COMMENT '平均评分',
    `review_user_count`        BIGINT COMMENT '评价用户数',
    `rate`        decimal(16,2) COMMENT '好评率'
) COMMENT '各课程评价统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_review_stats_by_course/';


//装载
insert overwrite table ads_review_stats_by_course
select *
from ads_review_stats_by_course
union
select
    '2022-06-08' dt,
    recent_days,
    course_id,
    course_name,
    cast(sum(sum_star)/sum(use_count) as bigint),
    sum(use_count),
    cast(sum(good_count)/sum(use_count) as decimal(16,2))
from dws_interaction_course_review_1d
lateral view explode(array(1,7,30)) tmp as recent_days
where dt>date_sub('2022-06-08',recent_days)
group by recent_days,course_id,course_name;




//1.3.5 各分类课程试听留存统计

DROP TABLE IF EXISTS ads_first_play_stats_by_category;
CREATE EXTERNAL TABLE ads_first_play_stats_by_category
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category_id`                   STRING COMMENT '分类ID',
    `category_name`                 STRING COMMENT '分类名称',
    `user_count`             BIGINT COMMENT '试听数',
    `rate`        decimal(16,2) COMMENT '比率'
) COMMENT '各分类课程试听留存统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_first_play_stats_by_category/';


//每日
insert overwrite table ads_first_play_stats_by_category
select *
from ads_first_play_stats_by_category
union
select
    '2022-06-08' dt,
    t1.recent_days,
    t3.category_id,
    t3.category_name,
    count(t1.user_id),
    cast(count(t2.user_id)/count(t1.user_id) as decimal(16,2))
from
    (select
         recent_days,
         course_id,
         user_id ,
         date_format(min(create_time),'yyyy-MM-dd') date_time
     from dwd_video_play_inc
              lateral view explode(array(1,2,3,4,5,6,7)) tmp as recent_days
     where dt>date_sub('2022-06-08',recent_days)
     group by recent_days,course_id,user_id) t1
        left join (
        select
            recent_days,
            course_id,
            user_id
        from dwd_trade_order_detail_inc
                 lateral view explode(array(1,2,3,4,5,6,7)) tmp as recent_days
        where dt>date_sub('2022-06-08',recent_days)
    ) t2 on t1.user_id=t2.user_id and t1.course_id=t2.course_id and t1.recent_days=t2.recent_days
         join dim_course_full t3
             on t1.course_id=t3.id
group by t1.recent_days,t3.category_id,t3.category_name;


//1.3.6 各学科课程试听留存统计

DROP TABLE IF EXISTS ads_first_play_stats_by_subject;
CREATE EXTERNAL TABLE ads_first_play_stats_by_subject
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `subject_id`                   STRING COMMENT '课程ID',
    `subject_name`                 STRING COMMENT '课程名称',
    `user_count`             BIGINT COMMENT '试听数',
    `rate`        decimal(16,2) COMMENT '比率'
) COMMENT '各学科课程试听留存统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_first_play_stats_by_subject/';


//每日
insert overwrite table ads_first_play_stats_by_subject
select *
from ads_first_play_stats_by_subject
union
select
    '2022-06-08' dt,
    t1.recent_days,
    t3.subject_id,
    t3.subject_name,
    count(t1.user_id),
    cast(count(t2.user_id)/count(t1.user_id) as decimal(16,2))
from
    (select
         recent_days,
         course_id,
         user_id ,
         date_format(min(create_time),'yyyy-MM-dd') date_time
     from dwd_video_play_inc
              lateral view explode(array(1,2,3,4,5,6,7)) tmp as recent_days
     where dt>date_sub('2022-06-08',recent_days)
     group by recent_days,course_id,user_id) t1
        left join (
        select
            recent_days,
            course_id,
            user_id
        from dwd_trade_order_detail_inc
                 lateral view explode(array(1,2,3,4,5,6,7)) tmp as recent_days
        where dt>date_sub('2022-06-08',recent_days)
    ) t2 on t1.user_id=t2.user_id and t1.course_id=t2.course_id and t1.recent_days=t2.recent_days
        join dim_course_full t3
             on t1.course_id=t3.id
group by t1.recent_days,t3.subject_id,t3.subject_name;


//1.3.7 各课程试听留存统计

DROP TABLE IF EXISTS ads_first_play_stats_by_course;
CREATE EXTERNAL TABLE ads_first_play_stats_by_course
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `course_id`                   STRING COMMENT '课程ID',
    `course_name`                 STRING COMMENT '课程name',
    `user_count`             BIGINT COMMENT '试听数',
    `rate`        decimal(16,2) COMMENT '比率'
) COMMENT '各课程试听留存统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_first_play_stats_by_course/';


//每日
insert overwrite table ads_first_play_stats_by_course
select *
from ads_first_play_stats_by_course
union
select
    '2022-06-08' dt,
    t1.recent_days,
    t1.course_id,
    t3.course_name,
    count(t1.user_id),
    cast(count(t2.user_id)/count(t1.user_id) as decimal(16,2))
from
(select
    recent_days,
    course_id,
    user_id ,
    date_format(min(create_time),'yyyy-MM-dd') date_time
from dwd_video_play_inc
    lateral view explode(array(1,2,3,4,5,6,7)) tmp as recent_days
where dt>date_sub('2022-06-08',recent_days)
group by recent_days,course_id,user_id) t1
left join (
    select
        recent_days,
        course_id,
        user_id
    from dwd_trade_order_detail_inc
             lateral view explode(array(1,2,3,4,5,6,7)) tmp as recent_days
    where dt>date_sub('2022-06-08',recent_days)
) t2 on t1.user_id=t2.user_id and t1.course_id=t2.course_id and t1.recent_days=t2.recent_days
join dim_course_full t3
on t1.course_id=t3.id
group by t1.recent_days,t1.course_id,t3.course_name;




--交易统计汇总
drop table if exists ads_order_all;
create external table ads_order_all
(
    dt                 string comment '统计日期',
    recent_days        bigint comment '最近天数1、7、30',
    order_total_amount decimal(16, 2) comment '下单总额',
    order_count        bigint comment '下单次数',
    order_user_count   bigint comment '下单人数'
) COMMENT '交易域综合汇总表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_order_all/';

insert overwrite table ads_order_all
select *
from ads_order_all
union
select '2022-06-08' dt,
       1            recent_days,
       sum(order_total_amount_1d),
       sum(order_count_1d),
       count(user_id)
from dws_trade_user_order_1d
where dt = '2022-06-08'
union
select '2022-06-08' dt,
       7            recent_days,
       sum(order_total_amount_1d),
       sum(order_count_1d),
       count(distinct user_id)
from dws_trade_user_order_1d
where dt >= date_add('2022-06-08', -6)
  and dt <= '2022-06-08'
union
select '2022-06-08' dt,
       30           recent_days,
       sum(order_total_amount_1d),
       sum(order_count_1d),
       count(distinct user_id)
from dws_trade_user_order_1d
where dt >= date_add('2022-06-08', -29)
  and dt <= '2022-06-08'

--交易各省份统计
DROP TABLE IF EXISTS ads_order_by_province;
CREATE EXTERNAL TABLE ads_order_by_province
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`      STRING COMMENT '省份ID',
    `province_name`    STRING COMMENT '省份名称',
    `area_code`        STRING COMMENT '地区编码',
    `iso_code`         STRING COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_code_3166_2`  STRING COMMENT '新版国际标准地区编码，供可视化使用',
    order_total_amount decimal(16, 2) comment '下单总额',
    order_count        bigint comment '下单次数',
    order_user_count   bigint comment '下单人数'
) COMMENT '各省份交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_order_by_province/';



insert overwrite table ads_order_by_province
select *
from ads_order_by_province
union
select '2022-06-08' dt,
       1            recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       sum(order_total_amount_1d),
       sum(order_count_1d),
       count(distinct user_id)
from dws_trade_user_sku_order_1d
where dt = '2022-06-08'
group by province_id,
         province_name,
         area_code,
         iso_code,
         iso_3166_2
union
select '2022-06-08' dt,
       recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       case recent_days
           when 7 then order_total_amount_7d
           when 30 then order_total_amount_30d
           end      order_total_amount,
       case recent_days
           when 7 then order_count_7d
           when 30 then order_count_30d
           end      order_count,
       case recent_days
           when 7 then order_user_count_7d
           when 30 then order_user_count_30d
           end      order_user_count
from dws_trade_province_order_nd lateral view explode(array(7, 30)) tmp as recent_days
where dt = '2022-06-08';



----------------------------1.5.1各试卷相关指标统计----------------------
DROP TABLE IF EXISTS ads_exam_stats_by_paper;
CREATE EXTERNAL TABLE ads_exam_stats_by_paper
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `paper_id`                   STRING COMMENT '试卷ID',
    `paper_name`                 STRING COMMENT '试卷名称',
    `avg_score`             BIGINT COMMENT '平均分',
    `avg_during_time`        BIGINT COMMENT '平均时长',
    user_num             BIGINT COMMENT '人数'
) COMMENT '各品牌商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_exam_stats_by_paper/';


insert overwrite table ads_exam_stats_by_paper
select * from ads_exam_stats_by_paper
union
select '2022-06-08',
       1,
       paper_id,
       paper_title,
       score_avg,
       duration_sec_avg,
       paper_num
from dws_exam_course_paper_1d
where dt = '2022-06-08'
union
select '2022-06-08',
       recent_days,
       paper_id,
       paper_title,
       case recent_days when 7 then score_avg_7d
                        when 30 then score_avg_30d end ,
       case recent_days when 7 then duration_sec_avg_7d
                        when 30 then duration_sec_avg_30d end,
       case recent_days when 7 then paper_num_7d
                        when 30 then paper_num_30d end
from dws_exam_course_paper_nd lateral view explode(array(7,30)) tmp as recent_days
where dt = '2022-06-08'

-------------------------------1.5.2各课程考试相关指标统计--------------------------------
DROP TABLE IF EXISTS ads_exam_course_stats_by_paper;
CREATE EXTERNAL TABLE ads_exam_course_stats_by_paper
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `course_id`                   STRING COMMENT '课程ID',
    `course_name`                 STRING COMMENT '课程名称',
    `avg_score`             BIGINT COMMENT '平均分',
    `avg_during_time`        BIGINT COMMENT '平均时长',
    user_num             BIGINT COMMENT '人数'
) COMMENT '各品牌商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_exam_course_stats_by_paper/';

insert overwrite table ads_exam_course_stats_by_paper
select *
from ads_exam_course_stats_by_paper
union
(select '2022-06-08',
        t.recent_days,
        t.course_id,
        t1.course_name,
        avg_score,
        avg_during_time,
        user_num
 from (select '2022-06-08',
              recent_days,
              course_id,
              cast(avg(score) as bigint)        avg_score,
              cast(avg(duration_sec) as bigint) avg_during_time,
              count(distinct user_id)           user_num
       from dwd_exam_paper_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
       where dt >= date_sub('2022-06-08', recent_days - 1)
         and dt <= '2022-06-08'
       group by recent_days, course_id) t
          left join (select id,
                            course_name
                     from dim_course_full
                     where dt = '2022-06-08') t1
                    on t.course_id = t1.id
);


----------------------1.5.3各试卷分数分布统计---------------------
DROP TABLE IF EXISTS ads_exam_score_stats_by_paper;
CREATE EXTERNAL TABLE ads_exam_score_stats_by_paper
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `paper_id`                   STRING COMMENT '试卷ID',
    `paper_title`                 STRING COMMENT '试卷名称',
    `pass_num`             BIGINT COMMENT '及格人数',
    `fail_num`        BIGINT COMMENT '不及格人数'
) COMMENT '各试卷分数分布统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_exam_score_stats_by_paper/';

insert overwrite table ads_exam_score_stats_by_paper
select * from ads_exam_score_stats_by_paper
union
select '2022-06-08',
       recent_days,
       paper_id,
       paper_title,
       sum(`if`(score >= 60,1,0)),
       sum(`if`(score < 60,1,0))
from dwd_exam_paper_inc lateral view explode(array(1,7,30)) tmp as recent_days
where dt >= date_sub('2022-06-08',recent_days - 1 ) and dt <= '2022-06-08'
group by recent_days,paper_id,paper_title;

-------------------------1.5.4各题目正确率统计---------------------------
DROP TABLE IF EXISTS ads_question_right_rate;
CREATE EXTERNAL TABLE ads_question_right_rate
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `question_id`                   STRING COMMENT '问题id',
    `right_rate`                   DECIMAL(16,2) COMMENT '正确率'
) COMMENT '各试卷分数分布统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_question_right_rate/';

insert overwrite table ads_question_right_rate
select * from ads_question_right_rate
union
select '2022-06-08',
       recent_days,
       question_id,
       cast(sum(sum_right_title)/sum(sum_title) as decimal(16,2))
from dws_exam_title_question_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt >= date_sub('2022-06-08',recent_days - 1) and dt <= '2022-06-08'
group by recent_days,question_id;



//1.6 播放主题

//1.6.1 各章节视频播放情况统计



DROP TABLE IF EXISTS ads_play_stats_by_chapter;
CREATE EXTERNAL TABLE ads_play_stats_by_chapter
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `chapter_id`                   STRING COMMENT '章节ID',
    `chapter_name`                 STRING COMMENT '章节name',
    `play_cnt`                 bigint COMMENT '视频播放次数',
    `avg_sec`             decimal(16,2)  COMMENT '人均观看时长',
    `user_count`        bigint COMMENT '观看人数'
) COMMENT '各章节视频播放情况统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_play_stats_by_chapter/';


insert overwrite table ads_play_stats_by_chapter
select
    *
from ads_play_stats_by_chapter
union
select
    dt, recent_days,chapter_id,chapter_name, video_play_cnt, avg_sec, user_count
from
(select
    '2022-06-08' dt,
    recent_days,
    chapter_id,
    count(distinct session_id) video_play_cnt,
    cast(sum(play_sec)/count(distinct user_id) as decimal(16,2)) avg_sec,
    count(distinct user_id) user_count
from dwd_video_play_inc
lateral view explode(array(1,7,30)) tmp as recent_days
where dt>date_sub('2022-06-08',recent_days)
group by recent_days,chapter_id) t1
join (
    select
        id,
        chapter_name
    from dim_chapter_full
    where dt='2022-06-08'
) t2 on t1.chapter_id=t2.id;



//1.6.2 各课程视频播放情况统计



DROP TABLE IF EXISTS ads_play_stats_by_course;
CREATE EXTERNAL TABLE ads_play_stats_by_course
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `course_id`                   STRING COMMENT '课程ID',
    `course_name`                 STRING COMMENT '课程name',
    `play_cnt`                 bigint COMMENT '视频播放次数',
    `avg_sec`             decimal(16,2)  COMMENT '人均观看时长',
    `user_count`        bigint COMMENT '观看人数'
) COMMENT '各课程视频播放情况统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_play_stats_by_course/';


insert overwrite table ads_play_stats_by_course
select
    *
from ads_play_stats_by_course
union
select
    dt, recent_days,t1.course_id,course_name, video_play_cnt, avg_sec, user_count
from
    (select
         '2022-06-08' dt,
         recent_days,
         course_id,
         count(distinct session_id) video_play_cnt,
         cast(sum(play_sec)/count(distinct user_id) as decimal(16,2)) avg_sec,
         count(distinct user_id) user_count
     from dwd_video_play_inc
              lateral view explode(array(1,7,30)) tmp as recent_days
     where dt>date_sub('2022-06-08',recent_days)
     group by recent_days,course_id) t1
        join (
        select
            course_id,
            course_name
        from dim_chapter_full
        where dt='2022-06-08'
    ) t2 on t1.course_id=t2.course_id;



--1.7.1各课程完课人数统计
drop table if exists ads_video_course_over;
create external table ads_video_course_over
(
    dt            string comment '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `course_id`   STRING COMMENT '课程ID',
    `course_name` STRING COMMENT '课程名',
    over_num      bigint comment '完课人数'
) COMMENT '各课程完课人数统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_video_course_over/';

insert overwrite table ads_video_course_over
select *
from ads_video_course_over
union
select '2022-06-08',
       1,
       course_id,
       course_name,
       count(distinct user_id)
from (
         select user_id,
                t1.course_id,
                course_name,
                chapter_user_num,
                chapter_num
         from (
                  select user_id,
                         course_id,
                         course_name,
                         count(*) chapter_user_num --该课程该用户完成章节数
                  from dws_video_chapter_user_play_td
                  where dt = '2022-06-08'
                    and over_data = '2022-06-08'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num
group by course_id, course_name
union
select '2022-06-08',
       7,
       course_id,
       course_name,
       count(distinct user_id)
from (
         select user_id,
                t1.course_id,
                course_name,
                chapter_user_num,
                chapter_num
         from (
                  select user_id,
                         course_id,
                         course_name,
                         count(*) chapter_user_num --该课程该用户完成章节数
                  from dws_video_chapter_user_play_td
                  where dt = '2022-06-08'
                    and over_data >= date_add('2022-06-08', -6)
                    and over_data <= '2022-06-08'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num
group by course_id, course_name
union
select '2022-06-08',
       30,
       course_id,
       course_name,
       count(distinct user_id)
from (
         select user_id,
                t1.course_id,
                course_name,
                chapter_user_num,
                chapter_num
         from (
                  select user_id,
                         course_id,
                         course_name,
                         count(*) chapter_user_num --该课程该用户完成章节数
                  from dws_video_chapter_user_play_td
                  where dt = '2022-06-08'
                    and over_data >= date_add('2022-06-08', -29)
                    and over_data <= '2022-06-08'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num
group by course_id, course_name;


--1.7.2 完课综合统计
drop table if exists ads_video_over;
create external table ads_video_over
(
    dt            string comment '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    over_num      bigint comment '完课人数',
    over_times    bigint comment '完课人次'
) COMMENT '各课程完课人数统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_video_over/';

insert overwrite table ads_video_over
select * from ads_video_over
union
select '2022-06-08',
       1,
       count(distinct user_id),
       count(*)
from (
         select user_id,
                t1.course_id,
                course_name,
                chapter_user_num,
                chapter_num
         from (
                  select user_id,
                         course_id,
                         course_name,
                         count(*) chapter_user_num --该课程该用户完成章节数
                  from dws_video_chapter_user_play_td
                  where dt = '2022-06-08'
                    and over_data = '2022-06-08'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num
union
select '2022-06-08',
       7,
       count(distinct user_id),
       count(*)
from (
         select user_id,
                t1.course_id,
                course_name,
                chapter_user_num,
                chapter_num
         from (
                  select user_id,
                         course_id,
                         course_name,
                         count(*) chapter_user_num --该课程该用户完成章节数
                  from dws_video_chapter_user_play_td
                  where dt = '2022-06-08'
                    and over_data >= date_add('2022-06-08', -6)
                    and over_data <= '2022-06-08'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num
union
select '2022-06-08',
       30,
       count(distinct user_id),
       count(*)
from (
         select user_id,
                t1.course_id,
                course_name,
                chapter_user_num,
                chapter_num
         from (
                  select user_id,
                         course_id,
                         course_name,
                         count(*) chapter_user_num --该课程该用户完成章节数
                  from dws_video_chapter_user_play_td
                  where dt = '2022-06-08'
                    and over_data >= date_add('2022-06-08', -29)
                    and over_data <= '2022-06-08'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num;


DROP TABLE IF EXISTS ads_trade_user;
CREATE EXTERNAL TABLE ads_trade_user
(
    `dt`              STRING COMMENT '统计日期',
    `retention_day`   INT COMMENT '近1，7，30日',
    `user_count` BIGINT COMMENT '总完课人数',
    course_cn  bigint comment '总完课人次'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_trade_user/';

insert overwrite table ads_trade_user
select * from ads_trade_user
union
select dt,
       days,
       count(distinct user_id)  user_count,
       count(user_id) course_cn
from (
         select user_id,
                course_id,
                dt,
                days,
                count(chapter_id) chapter_cn
         from dws_video_chapter_user_play_td lateral view explode(`array`(1,7,30)) tmp as days
         where dt="2022-06-08" and over_data=date_sub("2022-06-08",days-1)
         group by user_id,course_id,dt,days
     )t1
         left join (
    select course_id,
           count(id) chapter_cn
    from dim_chapter_full
    where dt="2022-06-08"
    group by course_id
)t2 on t1.course_id=t2.course_id
where t1.chapter_cn=t2.chapter_cn
group by dt, days;



--1.7.3人均完成章节统计
drop table if exists ads_video_course_over_avg;
create external table ads_video_course_over_avg
(
    dt            string comment '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `course_id`   STRING COMMENT '课程ID',
    `course_name` STRING COMMENT '课程名',
    over_avg      bigint comment '完课人数'
) COMMENT '各课程完课人数统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_video_course_over_avg/';

insert overwrite table ads_video_course_over_avg
select * from ads_video_course_over_avg
union
select '2022-06-08',
       1,
       course_id,
       course_name,
       sum(chapter_user_num)/count(distinct user_id)
from (
         select user_id,
                t1.course_id,
                course_name,
                chapter_user_num,
                chapter_num
         from (
                  select user_id,
                         course_id,
                         course_name,
                         count(*) chapter_user_num --该课程该用户完成章节数
                  from dws_video_chapter_user_play_td
                  where dt = '2022-06-08'
                    and over_data = '2022-06-08'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num
group by course_id, course_name
union
select '2022-06-08',
       7,
       course_id,
       course_name,
       sum(chapter_user_num)/count(distinct user_id)
from (
         select user_id,
                t1.course_id,
                course_name,
                chapter_user_num,
                chapter_num
         from (
                  select user_id,
                         course_id,
                         course_name,
                         count(*) chapter_user_num --该课程该用户完成章节数
                  from dws_video_chapter_user_play_td
                  where dt = '2022-06-08'
                    and over_data = over_data >= date_add('2022-06-08', -6)
                    and over_data <= '2022-06-08'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num
group by course_id, course_name
union
select '2022-06-08',
       30,
       course_id,
       course_name,
       sum(chapter_user_num)/count(distinct user_id)
from (
         select user_id,
                t1.course_id,
                course_name,
                chapter_user_num,
                chapter_num
         from (
                  select user_id,
                         course_id,
                         course_name,
                         count(*) chapter_user_num --该课程该用户完成章节数
                  from dws_video_chapter_user_play_td
                  where dt = '2022-06-08'
                    and over_data = over_data >= date_add('2022-06-08', -29)
                    and over_data <= '2022-06-08'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num
group by course_id, course_name




DROP TABLE IF EXISTS ads_course_avg_chapter;
CREATE EXTERNAL TABLE ads_course_avg_chapter
(
    `dt`              STRING COMMENT '统计日期',
    `retention_day`   INT COMMENT '近1，7，30日',
    `avg_chapter` BIGINT COMMENT '人均完成章节数'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/edu/ads/ads_course_avg_chapter/';


insert overwrite table ads_course_avg_chapter
select * from ads_course_avg_chapter
union
select "2022-06-08" dt,
       days retention_day,
       count(*)/count(distinct user_id) avg_chapter
from dws_video_chapter_user_play_td lateral view explode(`array`(1,7,30)) tmp as days
where dt="2022-06-08" and over_data >= date_sub("2022-06-08",days-1)
group by days;








