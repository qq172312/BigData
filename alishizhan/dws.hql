
--------------------1、流量域会话粒度页面浏览最近1日表------------------------

DROP TABLE IF EXISTS dws_traffic_session_page_view_1d;
CREATE EXTERNAL TABLE dws_traffic_session_page_view_1d
(
    `session_id`     STRING COMMENT '会话ID',
    `mid_id`         string comment '设备ID',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `version_code`   string comment 'APP版本号',
    `source_id`        string comment '渠道',
    `source_site`        string comment '渠道',
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
    `page_count_1d`  BIGINT COMMENT '最近1日浏览页面数'
) COMMENT '流量域会话粒度页面浏览最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_traffic_session_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--数据装载
insert overwrite table dws_traffic_session_page_view_1d partition(dt='2022-06-08')
select
    session_id,
    mid_id,
    brand,
    model,
    operate_system,
    version_code,
    source_id,
    source_site,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
where dt='2022-06-08'
group by session_id,mid_id,brand,model,operate_system,version_code,source_id,source_site;


---------------------交易域用户商品粒度订单最近1日表----------------------------

DROP TABLE IF EXISTS dws_trade_user_sku_order_1d;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_1d
(
    user_id                   STRING COMMENT '用户ID',
    course_id                    STRING COMMENT '课程ID',
    course_name      STRING COMMENT '课程名称',
    subject_id              STRING COMMENT '学科ID',
    subject_name     STRING COMMENT '学科名称',
    category_id            STRING COMMENT '分类id',
    category_name     STRING COMMENT '分类名称',
    province_id            STRING COMMENT '省份id',
    province_name          STRING COMMENT '省份名称',
    area_code             STRING COMMENT '地区编码',
    iso_code              STRING COMMENT '旧版国际标准地区编码',
    iso_3166_2             STRING COMMENT '新版国际标准地区编码',
    order_count_1d           BIGINT COMMENT '最近1日下单次数',
    order_original_amount_1d  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    coupon_reduce_amount_1d   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
    order_total_amount_1d     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_trade_user_sku_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


--首日装载

--首日装载

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = false;
insert overwrite table dws_trade_user_sku_order_1d partition(dt)
select     od.user_id           ,--        STRING COMMENT '用户ID',
           course_id                ,--    STRING COMMENT '课程ID',
           course_name,
           subject_id               ,--   STRING COMMENT '学科ID',
           subject_name,
           category_id              ,--  STRING COMMENT '分类id',
           category_name,
           province_id              ,--  STRING COMMENT '省份id',
           name,
           area_code,
           iso_code,
           iso_3166_2,
           order_count_1d           ,--    BIGINT COMMENT '最近1日下单次数',
           order_original_amount_1d ,-- DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
           coupon_reduce_amount_1d  ,-- DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
           order_total_amount_1d    ,-- DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
           dt
from (select user_id,
             course_id,
             province_id,
             count(*) order_count_1d,
             sum(origin_amount) order_original_amount_1d,
             sum(coupon_reduce) coupon_reduce_amount_1d,
             sum(final_amount) order_total_amount_1d,
             dt
      from dwd_trade_order_detail_inc
      group by user_id,course_id,province_id,dt) od
         left join (
    select id,
           course_name,
           subject_id,
           subject_name,
           category_id,
           category_name
    from dim_course_full
    where dt = '2022-06-08'
)cou on od.course_id = cou.id
         left join (
    select id,
           name,
           area_code,
           iso_code,
           iso_3166_2
    from dim_province_full
)pro on od.province_id = pro.id;
set hive.vectorized.execution.enabled = true;

set hive.vectorized.execution.enabled = false;
insert overwrite table dws_trade_user_sku_order_1d partition(dt='2022-06-09')
select     od.user_id           ,--        STRING COMMENT '用户ID',
           course_id                ,--    STRING COMMENT '课程ID',
           course_name,
           subject_id               ,--   STRING COMMENT '学科ID',
           subject_name,
           category_id              ,--  STRING COMMENT '分类id',
           category_name,
           province_id              ,--  STRING COMMENT '省份id',
           name,
           area_code,
           iso_code,
           iso_3166_2,
           order_count_1d           ,--    BIGINT COMMENT '最近1日下单次数',
           order_original_amount_1d ,-- DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
           coupon_reduce_amount_1d  ,-- DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
           order_total_amount_1d    -- DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
from (select user_id,
             course_id,
             province_id,
             count(*) order_count_1d,
             sum(origin_amount) order_original_amount_1d,
             sum(coupon_reduce) coupon_reduce_amount_1d,
             sum(final_amount) order_total_amount_1d
      from dwd_trade_order_detail_inc
      where dt = '2022-06-09'
      group by user_id,course_id,province_id,dt) od
         left join (
    select id,
           course_name,
           subject_id,
           subject_name,
           category_id,
           category_name
    from dim_course_full
    where dt = '2022-06-09'
)cou on od.course_id = cou.id
         left join (
    select id,
           name,
           area_code,
           iso_code,
           iso_3166_2
    from dim_province_full
)pro on od.province_id = pro.id
set hive.vectorized.execution.enabled = true;



-------------------------------3、交易域用户粒度订单最近1日表----------------------------

DROP TABLE IF EXISTS dws_trade_user_order_1d;
CREATE EXTERNAL TABLE dws_trade_user_order_1d
(
    `user_id`                   STRING COMMENT '用户ID',
    age                         STRING COMMENT '年龄',
    user_level                  STRING COMMENT '用户等级',
    gender                      STRING COMMENT '用户等级',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户粒度订单最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_trade_user_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日装载

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_order_1d partition(dt)
select     `user_id`           ,--        STRING COMMENT '用户ID',
           age                        ,-- STRING COMMENT '年龄',
           user_level                 ,-- STRING COMMENT '用户等级',
           gender                     ,-- STRING COMMENT '用户等级',
           `order_count_1d`           ,-- BIGINT COMMENT '最近1日下单次数',
           `order_original_amount_1d` ,-- DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
           `coupon_reduce_amount_1d`  ,-- DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
           `order_total_amount_1d`    ,-- DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
           dt
from(select user_id,
            count(distinct order_id) order_count_1d,
            sum(origin_amount) order_original_amount_1d,
            sum(coupon_reduce) coupon_reduce_amount_1d,
            sum(final_amount) order_total_amount_1d,
            dt
     from dwd_trade_order_detail_inc
     group by user_id, dt) od
        left join (
    select id,
           year('2022-06-08') - year(birthday) age,
           user_level,
           gender
    from dim_user_zip
    where dt = '9999-12-31'
) us on od.user_id = us.id;


--每日装载

insert overwrite table dws_trade_user_order_1d partition(dt = '2022-06-09')
select     `user_id`           ,--        STRING COMMENT '用户ID',
           age                        ,-- STRING COMMENT '年龄',
           user_level                 ,-- STRING COMMENT '用户等级',
           gender                     ,-- STRING COMMENT '用户等级',
           `order_count_1d`           ,-- BIGINT COMMENT '最近1日下单次数',
           `order_original_amount_1d` ,-- DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
           `coupon_reduce_amount_1d`  ,-- DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
           `order_total_amount_1d`    -- DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
from(select user_id,
            count(distinct order_id) order_count_1d,
            sum(origin_amount) order_original_amount_1d,
            sum(coupon_reduce) coupon_reduce_amount_1d,
            sum(final_amount) order_total_amount_1d
     from dwd_trade_order_detail_inc
     where dt = '2022-06-09') od
        left join (
    select id,
           year('2022-06-09') - year(birthday) age,
           user_level,
           gender
    from dim_user_zip
    where dt = '9999-12-31'
) us on od.user_id = us.id;









//4、交易域用户粒度支付最近1日表


DROP TABLE IF EXISTS dws_trade_user_payment_1d;
CREATE EXTERNAL TABLE dws_trade_user_payment_1d
(
    `user_id`           STRING COMMENT '用户ID',
    `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
    `payment_time`  STRING COMMENT '支付日期'
) COMMENT '交易域用户粒度支付最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_trade_user_payment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');



//首日装载
insert overwrite table dws_trade_user_payment_1d partition (dt)
select
    user_id,
    count(user_id),
    dt,
    dt
from dwd_trade_pay_detail_suc_inc
group by user_id,dt;


//每日装载
insert overwrite table dws_trade_user_payment_1d partition (dt='2022-06-09')
select
    user_id,
    count(user_id),
    dt
from dwd_trade_pay_detail_suc_inc
where dt='2022-06-09'
group by user_id;



//5、交易域用户粒度加购最近1日表

DROP TABLE IF EXISTS dws_trade_user_cart_add_1d;
CREATE EXTERNAL TABLE dws_trade_user_cart_add_1d
(
    `user_id`           STRING COMMENT '用户ID',
    `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数'
) COMMENT '交易域用户粒度加购最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_trade_user_cart_add_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//首日装载
insert overwrite table dws_trade_user_cart_add_1d partition (dt)
select
    user_id,
    count(user_id),
    dt
from dwd_trade_cart_add_inc
group by user_id,dt;


//每日

insert overwrite table dws_trade_user_cart_add_1d partition (dt='2022-06-09')
select
    user_id,
    count(user_id)
from dwd_trade_cart_add_inc
where dt='2022-06-09'
group by user_id;







--10 考试域课程试卷最近1日汇总表
--10 考试域课程试卷最近1日汇总表
drop table if exists dws_exam_course_paper_1d;
create external table dws_exam_course_paper_1d
(
    course_id        string comment '课程id',
    course_name      string comment '课程名称',
    subject_id       string comment '学科id',
    subject_name     string comment '科目名称',
    category_id      string comment '分类',
    category_name    string comment '分类名称',
    paper_id         string comment '试卷id',
    paper_title      string comment '试卷名称',
    paper_num        bigint comment '考试人数',
    score_total      bigint comment '试卷总分',
    score_avg        decimal(16, 2) comment '试卷平均分',
    duration_sec_sum bigint comment '总时长',
    duration_sec_avg decimal(16, 2) comment '平均时长'
) COMMENT '考试域课程试卷最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_exam_course_paper_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--一个课程一张试卷

--首日装载

--首日装载
insert overwrite table dws_exam_course_paper_1d partition (dt)
select t1.course_id,--string comment '课程id',
       course_name,--string comment '课程名称',
       subject_id,--string comment '学科id',
       subject_name,--string comment '科目名称',
       category_id,--string comment '分类',
       category_name,--string comment '分类名称',
       paper_id,--string comment '试卷id',
       paper_title,--string comment '试卷名称',
       paper_num,--bigint comment '考试人数',
       score_total,--bigint comment '试卷总分',
       score_avg,--decimal(16,2) comment '试卷平均分',
       duration_sec_sum,-- bigint comment '总时长',
       duration_sec_avg,--decimal(16,2) comment '平均时长'
       dt
from (
         select paper_id,
                course_id,
                paper_title,
                count(distinct user_id) paper_num,
                sum(score)              score_total,
                avg(score)              score_avg,
                sum(duration_sec)       duration_sec_sum,
                avg(duration_sec)       duration_sec_avg,
                dt
         from dwd_exam_paper_inc
         group by course_id, paper_id, paper_title, dt) t1
         left join(
    select id course_id,
           course_name,
           subject_id,
           subject_name,
           category_id,
           category_name
    from dim_course_full
    where dt = '2022-06-08'
) t2 on t1.course_id = t2.course_id;

--没日装载
insert overwrite table dws_exam_course_paper_1d partition (dt = '2022-06-08')
select t1.course_id,--string comment '课程id',
       course_name,--string comment '课程名称',
       subject_id,--string comment '学科id',
       subject_name,--string comment '科目名称',
       category_id,--string comment '分类',
       category_name,--string comment '分类名称',
       paper_id,--string comment '试卷id',
       paper_title,--string comment '试卷名称',
       paper_num,--bigint comment '考试人数',
       score_total,--bigint comment '试卷总分',
       score_avg,--decimal(16,2) comment '试卷平均分',
       duration_sec_sum,-- bigint comment '总时长',
       duration_sec_avg--decimal(16,2) comment '平均时长'
from (
         select paper_id,
                course_id,
                paper_title,
                count(distinct user_id) paper_num,
                sum(score)              score_total,
                avg(score)              score_avg,
                sum(duration_sec)       duration_sec_sum,
                avg(duration_sec)       duration_sec_avg
         from dwd_exam_paper_inc
         where dt = '2022-06-08'
         group by course_id, paper_id, paper_title) t1
         left join(
    select id course_id,
           course_name,
           subject_id,
           subject_name,
           category_id,
           category_name
    from dim_course_full
    where dt = '2022-06-08'
) t2 on t1.course_id = t2.course_id;





--12 流量域访客页面粒度页面浏览最近1日表
DROP TABLE IF EXISTS dws_traffic_page_visitor_page_view_1d;
CREATE EXTERNAL TABLE dws_traffic_page_visitor_page_view_1d
(
    `mid_id`         STRING COMMENT '访客ID',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `page_id`        STRING COMMENT '页面ID',
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
    `view_count_1d`  BIGINT COMMENT '最近1日访问次数'
) COMMENT '流量域访客页面粒度页面浏览最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_traffic_page_visitor_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


--数据装载
insert overwrite table dws_traffic_page_visitor_page_view_1d partition (dt = '2022-06-08')
select mid_id,
       brand,
       model,
       operate_system,
       page_id,
       sum(during_time),
       count(*)
from dwd_traffic_page_view_inc
where dt = '2022-06-08'
group by mid_id,brand,model,operate_system,page_id;



//13、互动域各课程粒度评价最近1日表

DROP TABLE IF EXISTS dws_interaction_course_review_1d;
CREATE EXTERNAL TABLE dws_interaction_course_review_1d
(
    `course_id`         STRING COMMENT '课程',
    `course_name`   STRING COMMENT '课程名字',
    `sum_star`          string comment '求和评分',
    `use_count`          string comment '评价用户数',
    `good_count` string comment '好评数'
) COMMENT '互动域各课程粒度评价最近1日表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_interaction_course_review_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//首日
insert overwrite table dws_interaction_course_review_1d partition (dt)
select
    course_id,
    t2.course_name,
    sum(review_stars),
    count(distinct user_id),
    count(`if`(review_stars=5,1,null)),
    t1.dt
from dwd_interaction_course_review_inc t1 join dim_course_full t2
on t1.course_id=t2.id
group by t1.dt,course_id,t2.course_name;



//每日
insert overwrite table dws_interaction_course_review_1d partition (dt='2022-06-08')
select
    course_id,
    t2.course_name,
    sum(review_stars),
    count(distinct user_id),
    count(`if`(review_stars=5,1,null))
from dwd_interaction_course_review_inc t1 join dim_course_full t2
                                               on t1.course_id=t2.id
where t1.dt='2022-06-09' and t2.dt='2022-06-09'
group by t1.dt,course_id,t2.course_name;



----------------------------交易域用户课程粒度订单最近n日汇总表-------------------------------
DROP TABLE IF EXISTS dws_trade_user_sku_order_nd;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_nd
(
    user_id                   STRING COMMENT '用户ID',
    course_id                    STRING COMMENT '课程ID',
    course_name          STRING COMMENT '课程名称',
    subject_id              STRING COMMENT '学科ID',
    subject_name          STRING COMMENT '学科名称',
    category_id            STRING COMMENT '分类id',
    category_name         STRING COMMENT '分类名称',
    province_id            STRING COMMENT '省份id',
    order_count_7d           BIGINT COMMENT '最近7日下单次数',
    order_original_amount_7d  DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    coupon_reduce_amount_7d   DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额',
    order_total_amount_7d     DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    order_count_30d           BIGINT COMMENT '最近30日下单次数',
    order_original_amount_30d  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    coupon_reduce_amount_30d   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',
    order_total_amount_30d     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近n日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_trade_user_sku_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--数据装载
--首日装载

--数据装载
insert overwrite table dws_trade_user_sku_order_nd partition (dt='2022-06-08')
select user_id,
       course_id,
       course_name,
       subject_id,
       subject_name,
       category_id,
       category_name,
       province_id,
       sum(`if`(dt >= date_sub('2022-06-08',6),order_count_1d,0)) order_count_7d,
       sum(`if`(dt >= date_sub('2022-06-08',6),order_original_amount_1d,0)) order_original_amount_7d,
       sum(`if`(dt >= date_sub('2022-06-08',6),coupon_reduce_amount_1d,0)) coupon_reduce_amount_7d,
       sum(`if`(dt >= date_sub('2022-06-08',6),order_total_amount_1d,0)) order_total_amount_7d,
       sum(order_count_1d),
       sum(order_original_amount_1d),
       sum(coupon_reduce_amount_1d),
       sum(order_total_amount_1d)
from dws_trade_user_sku_order_1d
where dt >= date_sub('2022-06-08',29) and dt <= '2022-06-08'
group by user_id,
         course_id,
         course_name,
         subject_id,
         subject_name,
         category_id,
         category_name,
         province_id;


set hive.vectorized.execution.enabled = false;
insert overwrite table dws_trade_user_sku_order_1d partition(dt='2022-06-09')
select     od.user_id           ,--        STRING COMMENT '用户ID',
           course_id                ,--    STRING COMMENT '课程ID',
           course_name,
           subject_id               ,--   STRING COMMENT '学科ID',
           subject_name,
           category_id              ,--  STRING COMMENT '分类id',
           category_name,
           province_id              ,--  STRING COMMENT '省份id',
           name,
           area_code,
           iso_code,
           iso_3166_2,
           order_count_1d           ,--    BIGINT COMMENT '最近1日下单次数',
           order_original_amount_1d ,-- DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
           coupon_reduce_amount_1d  ,-- DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
           order_total_amount_1d    -- DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
from (select user_id,
             course_id,
             province_id,
             count(*) order_count_1d,
             sum(origin_amount) order_original_amount_1d,
             sum(coupon_reduce) coupon_reduce_amount_1d,
             sum(final_amount) order_total_amount_1d
      from dwd_trade_order_detail_inc
      where dt = '2022-06-09'
      group by user_id,course_id,province_id,dt) od
         left join (
    select id,
           course_name,
           subject_id,
           subject_name,
           category_id,
           category_name
    from dim_course_full
    where dt = '2022-06-09'
)cou on od.course_id = cou.id
         left join (
    select id,
           name,
           area_code,
           iso_code,
           iso_3166_2
    from dim_province_full
)pro on od.province_id = pro.id;
set hive.vectorized.execution.enabled = true;





-----------------------------交易域省份粒度订单最近n日汇总表-----------------------
-----------------------------交易域省份粒度订单最近n日汇总表-----------------------
DROP TABLE IF EXISTS dws_trade_province_order_nd;
CREATE EXTERNAL TABLE dws_trade_province_order_nd
(
    `province_id`               STRING COMMENT '省份ID',
    `province_name`             STRING COMMENT '省份名称',
    `area_code`                 STRING COMMENT '地区编码',
    `iso_code`                  STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                STRING COMMENT '新版国际标准地区编码',
    order_user_count_7d         bigint comment '最近7日下单人数',
    `order_count_7d`            BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `coupon_reduce_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`     DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    order_user_count_30d        bigint comment '最近30日下单人数',
    `order_count_30d`           BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `coupon_reduce_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`    DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_trade_province_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--数据装载
insert overwrite table dws_trade_province_order_nd partition (dt = '2022-06-08')
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       sum(`if`(dt >= date_sub('2022-06-08',6),order_count_1d,0)),
       sum(`if`(dt >= date_sub('2022-06-08',6),order_original_amount_1d,0)),
       sum(`if`(dt >= date_sub('2022-06-08',6),coupon_reduce_amount_1d,0)),
       sum(`if`(dt >= date_sub('2022-06-08',6),order_total_amount_1d,0)),
       sum(order_count_1d),
       sum(order_original_amount_1d),
       sum(coupon_reduce_amount_1d),
       sum(order_total_amount_1d)
from dws_trade_user_sku_order_1d
where dt >= date_sub('2022-06-08',29) and dt < '2022-06-08'
group by province_id,
         province_name,
         area_code,
         iso_code,
         iso_3166_2


-------------------------用户订单历史至今表--------------------------------
DROP TABLE IF EXISTS dws_trade_user_order_td;
CREATE EXTERNAL TABLE dws_trade_user_order_td
(
    `user_id`                   STRING COMMENT '用户ID',
    `order_date_first`          STRING COMMENT '历史至今首次下单日期',
    `order_date_last`           STRING COMMENT '历史至今末次下单日期',
    `pay_date_first`            STRING COMMENT '历史至今首次支付日期',
    `pay_date_last`             STRING COMMENT '历史至今末次支付日期',
    `order_count_td`            BIGINT COMMENT '历史至今下单次数',
    `original_amount_td`        DECIMAL(16, 2) COMMENT '历史至今下单原始金额',
    `coupon_reduce_amount_td`   DECIMAL(16, 2) COMMENT '历史至今下单优惠券优惠金额',
    `total_amount_td`           DECIMAL(16, 2) COMMENT '历史至今下单最终金额'
) COMMENT '交易域用户粒度订单历史至今汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_trade_user_order_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日装载
insert overwrite table dws_trade_user_order_td partition (dt = '2022-06-08')
select     od.`user_id`          ,--         STRING COMMENT '用户ID',
           `order_date_first`        ,--  STRING COMMENT '历史至今首次下单日期',
           `order_date_last`         ,--  STRING COMMENT '历史至今末次下单日期',
           `pay_date_first`          ,--  STRING COMMENT '历史至今首次支付日期',
           `pay_date_last`           ,--  STRING COMMENT '历史至今末次支付日期',
           `order_count_td`          ,--  BIGINT COMMENT '历史至今下单次数',
           `original_amount_td`      ,--  DECIMAL(16, 2) COMMENT '历史至今下单原始金额',
           `coupon_reduce_amount_td` ,--  DECIMAL(16, 2) COMMENT '历史至今下单优惠券优惠金额',
           `total_amount_td`         --  DECIMAL(16, 2) COMMENT '历史至今下单最终金额'
from (select user_id,
             min(dt) order_date_first,
             max(dt) order_date_last,
             sum(order_count_1d) order_count_td,
             sum(order_original_amount_1d) original_amount_td,
             sum(coupon_reduce_amount_1d) coupon_reduce_amount_td,
             sum(order_total_amount_1d) total_amount_td
      from dws_trade_user_order_1d
      group by user_id) od
         left join (
    select user_id,
           min(dt) pay_date_first,
           max(dt) pay_date_last
    from dws_trade_user_payment_1d
    group by user_id
) pay on od.user_id = pay.user_id;

--每日装载
insert overwrite table dws_trade_user_order_td partition (dt = '2022-06-09')
select user_id,
       min(order_date_first),
       max(order_date_last),
       min(pay_date_first),
       max(pay_date_last),
       sum(order_count_td),
       sum(original_amount_td),
       sum(coupon_reduce_amount_td),
       sum(total_amount_td)
from (
         select user_id,
                order_date_first,
                order_date_last,
                pay_date_first,
                pay_date_last,
                order_count_td,
                original_amount_td,
                coupon_reduce_amount_td,
                total_amount_td
         from dws_trade_user_order_td
         where dt = date_sub('2022-06-09',1)
         union all
         select user_id,
                '2022-06-09' order_date_first,
                '2022-06-09' order_date_last,
                '2022-06-09' pay_date_first,
                '2022-06-09' pay_date_last,
                order_count_1d,
                order_original_amount_1d,
                coupon_reduce_amount_1d,
                order_total_amount_1d
         from dws_trade_user_order_1d
         where dt = '2022-06-09')t1
group by user_id;


-----------------------用户登录历史至今表----------------------------------
DROP TABLE IF EXISTS dws_user_user_login_td;
CREATE EXTERNAL TABLE dws_user_user_login_td
(
    `user_id`          STRING COMMENT '用户ID',
    `login_date_last`  STRING COMMENT '历史至今末次登录日期',
    `login_date_first` STRING COMMENT '历史至今首次登录日期',
    `login_count_td`   BIGINT COMMENT '历史至今累计登录次数'
) COMMENT '用户域用户粒度登录历史至今汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_user_user_login_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日装载
insert overwrite table dws_user_user_login_td partition (dt = '2022-06-08')
select u.id,
       nvl(login_date_last,date_format(create_time,'yyyy-MM-dd')),
       date_format(create_time,'yyyy-MM-dd'),
       nvl(login_count_td,1)
from (
         select id,
                create_time
         from dim_user_zip
         where dt = '9999-12-31'
     ) u
         left join (
    select user_id,
           max(dt) login_date_last,
           count(*) login_count_td
    from dwd_user_login_inc
    group by user_id
) log on u.id = log.user_id;

--每日装载
select nvl(old.user_id,new.user_id) ,
       `if`(new.user_id is null ,old.login_date_last,'2022-06-09'),
       `if`(old.login_date_first is null ,'2022-06-09',old.login_date_first),
       nvl(old.login_count_td,0) + nvl(new.login_count_1d,0)
from (
         select user_id,
                login_date_last,
                login_date_first,
                login_count_td
         from dws_user_user_login_td
         where dt = date_sub('2022-06-09',1)
     )old
         full outer join (
    select user_id,
           count(*) login_count_1d
    from dwd_user_login_inc
    where dt = '2022-06-09'
    group by user_id
)new
                         on old.user_id = new.user_id;




//视频域各用户各章节完播历史至今表
DROP TABLE IF EXISTS dws_video_chapter_user_play_td;
CREATE EXTERNAL TABLE dws_video_chapter_user_play_td
(
    `user_id`          STRING COMMENT '用户ID',
    `course_id` STRING COMMENT '课程ID',
    `course_name` STRING COMMENT '课程名',
    `chapter_id`  STRING COMMENT '章节id',
    `chapter_name`  STRING COMMENT '章节名',
    `over_data` STRING COMMENT '首次完播日期'
) COMMENT '视频域各用户各章节完播历史至今表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_video_chapter_user_play_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');



//首日
insert overwrite table dws_video_chapter_user_play_td partition (dt='2022-06-08')
select
    user_id,
    t1.course_id,
    t2.course_name,
    t1.chapter_id,
    t2.chapter_name,
    min(create_time)
from
(
    select user_id,
           chapter_id,
           course_id,
           video_id,
           play_sec,
           position_sec,
           create_time
    from dwd_video_play_full
) t1
join (
    select
        video_id,
        chapter_name,
        course_id,
        course_name,
        video_name,
        during_sec
    from dim_chapter_full
    where dt='2022-06-08'
) t2 on t1.video_id=t2.video_id
where play_sec>(during_sec*0.9) and position_sec>(during_sec*0.9)
group by user_id, t1.course_id, t2.course_name, t1.chapter_id, t2.chapter_name;



//每日
insert overwrite table dws_video_chapter_user_play_td partition (dt='2022-06-09')
select *
from dws_video_chapter_user_play_td
union
select
    t4.user_id,
    t4.course_id,
    t3.course_name,
    t4.chapter_id,
    t3.chapter_name,
    min(t4.create_time)
from
    (
        select t1.user_id,
               t1.chapter_id,
               t1.course_id,
               video_id,
               play_sec,
               position_sec,
               t1.create_time
        from dwd_video_play_full t1 left join dws_video_chapter_user_play_td t2
                                              on t1.user_id =t2.user_id and t1.chapter_id=t2.chapter_id
        where t1.dt='2022-06-09' and t2.user_id is null
    ) t4
        join (
        select
            video_id,
            chapter_name,
            course_id,
            course_name,
            video_name,
            during_sec
        from dim_chapter_full
        where dt='2022-06-08'
    ) t3 on t4.video_id=t3.video_id
where play_sec>(during_sec*0.9) and position_sec>(during_sec*0.9)
group by t4.user_id, t4.course_id, t3.course_name, t4.chapter_id, t3.chapter_name;







--考试域题目粒度答题最近1日汇总表
drop table if exists dws_exam_title_question_1d;
create external table  dws_exam_title_question_1d (
                                                      `question_id`      STRING COMMENT '题目id',
                                                      sum_right_title bigint comment '近一日正确答题数',
                                                      sum_title bigint comment '近一日总答题数'
)comment '考试域题目粒度答题最近1日汇总表'
    partitioned by (dt string)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_exam_title_question_id'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_exam_title_question_1d partition (dt)
select question_id,
       sum(`if`(is_correct=1,1,0)) sum_right_title,
       count(question_id) sum_title,
       dt
from dwd_exam_question_inc
group by question_id, dt;

--每日
insert overwrite table dws_exam_title_question_1d partition (dt="2022-06-09")
select question_id,
       sum(`if`(is_correct=1,1,0)) sum_right_title,
       count(question_id) sum_title
from dwd_exam_question_inc
where dt = "2022-06-09"
group by question_id;




--学习域用户章节粒度视频播放最近1汇总日表
drop table if exists dws_study_course_video_play_1d;
create external table dws_study_course_video_play_1d(
                                                        chapter_id      string comment '章节id',
                                                        `course_id`      STRING COMMENT '课程id',
                                                        user_id string comment '用户',
                                                        video_play_id bigint comment '近1日视频播放次数',
                                                        duration_time bigint comment '近1日观看总时长'
)comment '学习域课程粒度视频播放最近1日表'
    partitioned by (dt string)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_study_course_video_play_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


--首日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_study_course_video_play_1d partition (dt)
select chapter_id,
       course_id,
       user_id,
       count(session_id) video_play_id,
       sum(play_sec) duration_time,
       dt
from dwd_video_play_inc
group by chapter_id, course_id, user_id,dt;

--每日
insert overwrite table dws_study_course_video_play_1d partition (dt="2022-06-09")
select chapter_id,
       course_id,
       user_id,
       count(session_id) video_play_id,
       sum(play_sec) duration_time
from dwd_video_play_inc
where dt = "2022-06-09"
group by chapter_id, course_id,user_id;



--13 考试域课程试卷最近n日汇总表
drop table if exists dws_exam_course_paper_nd;
create external table dws_exam_course_paper_nd
(
    course_id            string comment '课程id',
    course_name          string comment '课程名称',
    subject_id           string comment '学科id',
    subject_name         string comment '科目名称',
    category_id          string comment '分类',
    category_name        string comment '分类名称',
    paper_id             string comment '试卷id',
    paper_title          string comment '试卷名称',
    paper_num_7d         bigint comment '考试人数',
    score_total_7d       bigint comment '试卷总分',
    score_avg_7d         decimal(16, 2) comment '试卷平均分',
    duration_sec_sum_7d  bigint comment '总时长',
    duration_sec_avg_7d  decimal(16, 2) comment '平均时长',
    paper_num_30d        bigint comment '考试人数',
    score_total_30d      bigint comment '试卷总分',
    score_avg_30d        decimal(16, 2) comment '试卷平均分',
    duration_sec_sum_30d bigint comment '总时长',
    duration_sec_avg_30d decimal(16, 2) comment '平均时长'
) COMMENT '考试域课程试卷最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dws/dws_exam_course_paper_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日装载
insert overwrite table dws_exam_course_paper_nd partition (dt = '2022-06-08')
select t1.course_id,--string comment '课程id',
       course_name,--string comment '课程名称',
       subject_id,--string comment '学科id',
       subject_name,--string comment '科目名称',
       category_id,--string comment '分类',
       category_name,--string comment '分类名称',
       paper_id,--string comment '试卷id',
       paper_title,--string comment '试卷名称',
       paper_num_7d,--bigint comment '考试人数',
       score_total_7d,--bigint comment '试卷总分',
       score_avg_7d,--decimal(16, 2) comment '试卷平均分',
       duration_sec_sum_7d,--bigint comment '总时长',
       duration_sec_avg_7d,--decimal(16, 2) comment '平均时长',
       paper_num_30d,-- bigint comment '考试人数',
       score_total_30d,-- bigint comment '试卷总分',
       score_avg_30d,-- decimal(16, 2) comment '试卷平均分',
       duration_sec_sum_30d,-- bigint comment '总时长',
       duration_sec_avg_30d-- decimal(16, 2) comment '平均时长'
from (
         select paper_id,
                course_id,
                paper_title,
                count(distinct if(dt >= date_add('2022-06-08', -6), user_id, null)) paper_num_7d,
                sum(if(dt >= date_add('2022-06-08', -6), score, 0))                 score_total_7d,
                avg(if(dt >= date_add('2022-06-08', -6), score, 0))                 score_avg_7d,
                sum(if(dt >= date_add('2022-06-08', -6), duration_sec, 0))          duration_sec_sum_7d,
                avg(if(dt >= date_add('2022-06-08', -6), duration_sec, 0))          duration_sec_avg_7d,
                count(distinct user_id)                                             paper_num_30d,
                sum(score)                                                          score_total_30d,
                avg(score)                                                          score_avg_30d,
                sum(duration_sec)                                                   duration_sec_sum_30d,
                avg(duration_sec)                                                   duration_sec_avg_30d
         from dwd_exam_paper_inc lateral view explode(array(7, 30)) tmp as recent_days
         where dt >= date_add('2022-06-08', -29)
           and dt <= '2022-06-08'
         group by course_id, paper_id, paper_title) t1
         left join(
    select id course_id,
           course_name,
           subject_id,
           subject_name,
           category_id,
           category_name
    from dim_course_full
    where dt = '2022-06-08'
) t2 on t1.course_id = t2.course_id;