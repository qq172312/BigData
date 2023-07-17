//10.1 最近1日汇总表

//10.1.1 交易域用户商品粒度订单最近1日汇总表

DROP TABLE IF EXISTS dws_trade_user_sku_order_1d;

CREATE EXTERNAL TABLE dws_trade_user_sku_order_1d
(
    `user_id`                   STRING COMMENT '用户ID',
    `sku_id`                    STRING COMMENT 'SKU_ID',
    `sku_name`                  STRING COMMENT 'SKU名称',
    `category1_id`              STRING COMMENT '一级品类ID',
    `category1_name`            STRING COMMENT '一级品类名称',
    `category2_id`              STRING COMMENT '二级品类ID',
    `category2_name`            STRING COMMENT '二级品类名称',
    `category3_id`              STRING COMMENT '三级品类ID',
    `category3_name`            STRING COMMENT '三级品类名称',
    `tm_id`                     STRING COMMENT '品牌ID',
    `tm_name`                   STRING COMMENT '品牌名称',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//代码
insert overwrite table dws_trade_user_sku_order_1d partition (dt)
select user_id,
       dwd_od.sku_id,
       dim_sku.sku_name,
       dim_sku.category1_id,
       dim_sku.category1_name,
       dim_sku.category2_id,
       dim_sku.category2_name,
       dim_sku.category3_id,
       dim_sku.category3_name,
       tm_id,
       tm_name,
       order_count_1d,
       order_num_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d,
       date_id
from (select user_id,
             sku_id,
             sum(split_original_amount) over (partition by user_id,sku_id) order_original_amount_1d,
             sum(split_activity_amount) over (partition by user_id,sku_id) activity_reduce_amount_1d,
             sum(split_coupon_amount) over (partition by user_id,sku_id)   coupon_reduce_amount_1d,
             sum(split_total_amount) over (partition by user_id,sku_id)    order_total_amount_1d,
             count(1) over (partition by user_id,sku_id)                   order_count_1d,
             sum(sku_num) over (partition by user_id,sku_id)               order_num_1d,
             row_number() over (partition by user_id,sku_id)               rn,
             date_id
      from dwd_trade_order_detail_inc
      where dt = '2022-06-09') dwd_od
         join
     (select id,
             sku_name,
             category1_id,
             category1_name,
             category2_id,
             category2_name,
             category3_id,
             category3_name,
             tm_id,
             tm_name
      from dim_sku_full
      where dt = '2022-06-08') dim_sku
     on dwd_od.sku_id = dim_sku.id and rn = 1;



//10.1.2 交易域用户粒度订单最近1日汇总表

DROP TABLE IF EXISTS dws_trade_user_order_1d;
CREATE EXTERNAL TABLE dws_trade_user_order_1d
(
    `user_id`                   STRING COMMENT '用户ID',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单商品件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户粒度订单最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//首日装载
insert overwrite table dws_trade_user_order_1d partition (dt = '2022-06-08')
select user_id,
       sum(order_count_1d),
       sum(order_num_1d),
       sum(order_original_amount_1d),
       sum(activity_reduce_amount_1d),
       sum(coupon_reduce_amount_1d),
       sum(order_total_amount_1d)
from dws_trade_user_sku_order_1d
where dt = '2022-06-08'
group by user_id;


//10.1.3 交易域用户粒度加购最近1日汇总表

DROP TABLE IF EXISTS dws_trade_user_cart_add_1d;
CREATE EXTERNAL TABLE dws_trade_user_cart_add_1d
(
    `user_id`           STRING COMMENT '用户ID',
    `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数',
    `cart_add_num_1d`   BIGINT COMMENT '最近1日加购商品件数'
) COMMENT '交易域用户粒度加购最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_cart_add_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//每日装载
insert overwrite table dws_trade_user_cart_add_1d partition (dt)
select user_id,
       count(id),
       sum(sku_num),
       collect_set(dt)[0]
from dwd_trade_cart_add_inc
where dt = '2022-06-08'
group by user_id;


//10.1.4 交易域用户粒度支付最近1日汇总表

DROP TABLE IF EXISTS dws_trade_user_payment_1d;
CREATE EXTERNAL TABLE dws_trade_user_payment_1d
(
    `user_id`           STRING COMMENT '用户ID',
    `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
    `payment_num_1d`    BIGINT COMMENT '最近1日支付商品件数',
    `payment_amount_1d` DECIMAL(16, 2) COMMENT '最近1日支付金额'
) COMMENT '交易域用户粒度支付最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_payment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//每日装载

insert overwrite table dws_trade_user_payment_1d partition (dt)
select user_id,
       count(id),
       sum(sku_num),
       sum(split_payment_amount),
       collect_set(dt)[0]
from dwd_trade_pay_detail_suc_inc
where dt = '2022-06-08'
group by user_id;


//10.1.5 交易域省份粒度订单最近1日汇总表

DROP TABLE IF EXISTS dws_trade_province_order_1d;
CREATE EXTERNAL TABLE dws_trade_province_order_1d
(
    `province_id`               STRING COMMENT '省份ID',
    `province_name`             STRING COMMENT '省份名称',
    `area_code`                 STRING COMMENT '地区编码',
    `iso_code`                  STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                STRING COMMENT '新版国际标准地区编码',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域省份粒度订单最近1日汇总表'
    PARTITIONED BY (`dt` STRING);


//每日装载
insert overwrite table dws_trade_province_order_1d partition (dt)
select `province_id`,-- STRING COMMENT '省份ID',
       `province_name`,--     STRING COMMENT '省份名称',
       `area_code`,--     STRING COMMENT '地区编码',
       `iso_code`,--     STRING COMMENT '旧版国际标准地区编码',
       `iso_3166_2`,--     STRING COMMENT '新版国际标准地区编码',
       `order_count_1d`,--     BIGINT COMMENT '最近1日下单次数',
       `order_original_amount_1d`,-- DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
       `activity_reduce_amount_1d`,-- DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
       `coupon_reduce_amount_1d`,-- DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
       `order_total_amount_1d`, -- DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
       dt
from (select province_id,
             count(id)                  order_count_1d,
             sum(split_original_amount) order_original_amount_1d,
             sum(split_activity_amount) activity_reduce_amount_1d,
             sum(split_coupon_amount)   coupon_reduce_amount_1d,
             sum(split_total_amount)    order_total_amount_1d,
             collect_set(dt)[0]         dt
      from dwd_trade_order_detail_inc
      where dt = '2022-06-08'
      group by province_id) t1
         join (select id,
                      province_name,
                      area_code,
                      iso_code,
                      iso_3166_2
               from dim_province_full
               where dt = '2022-06-09') t2 on t1.province_id = t2.id;


//10.1.6 工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表

DROP TABLE IF EXISTS dws_tool_user_coupon_coupon_used_1d;
CREATE EXTERNAL TABLE dws_tool_user_coupon_coupon_used_1d
(
    `user_id`          STRING COMMENT '用户ID',
    `coupon_id`        STRING COMMENT '优惠券ID',
    `coupon_name`      STRING COMMENT '优惠券名称',
    `coupon_type_code` STRING COMMENT '优惠券类型编码',
    `coupon_type_name` STRING COMMENT '优惠券类型名称',
    `benefit_rule`     STRING COMMENT '优惠规则',
    `used_count_1d`    STRING COMMENT '使用(支付)次数'
) COMMENT '工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_tool_user_coupon_coupon_used_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//每日装载
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition (dt = '2022-06-08')
select t1.user_id,
       t2.*,
       t1.used_count_1d
from (select user_id,
             coupon_id,
             count(*) used_count_1d
      from dwd_tool_coupon_used_inc
      where dt = '2022-06-08'
      group by user_id, coupon_id) t1
         join (select id,
                      coupon_name,
                      coupon_type_code,
                      coupon_type_name,
                      benefit_rule
               from dim_coupon_full
               where dt = '2022-06-08') t2
              on t1.coupon_id = t2.id


//10.1.7 互动域商品粒度收藏商品最近1日汇总表


DROP TABLE IF EXISTS dws_interaction_sku_favor_add_1d;
CREATE EXTERNAL TABLE dws_interaction_sku_favor_add_1d
(
    `sku_id`             STRING COMMENT 'SKU_ID',
    `sku_name`           STRING COMMENT 'SKU名称',
    `category1_id`       STRING COMMENT '一级品类ID',
    `category1_name`     STRING COMMENT '一级品类名称',
    `category2_id`       STRING COMMENT '二级品类ID',
    `category2_name`     STRING COMMENT '二级品类名称',
    `category3_id`       STRING COMMENT '三级品类ID',
    `category3_name`     STRING COMMENT '三级品类名称',
    `tm_id`              STRING COMMENT '品牌ID',
    `tm_name`            STRING COMMENT '品牌名称',
    `favor_add_count_1d` BIGINT COMMENT '商品被收藏次数'
) COMMENT '互动域商品粒度收藏商品最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_interaction_sku_favor_add_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//每日装载
insert overwrite table dws_interaction_sku_favor_add_1d partition (dt)
select t2.*,
       t1.favor_add_count_1d,
       dt
from (select sku_id,
             count(sku_id)      favor_add_count_1d,
             collect_set(dt)[0] dt
      from dwd_interaction_favor_add_inc
      where dt = '2022-06-08'
      group by sku_id) t1
         join (select id,
                      sku_name,
                      category1_id,
                      category1_name,
                      category2_id,
                      category2_name,
                      category3_id,
                      category3_name,
                      tm_id,
                      tm_name
               from dim_sku_full
               where dt = '2022-06-08') t2
              on t1.sku_id = t2.id;


//10.1.8 流量域会话粒度页面浏览最近1日汇总表

DROP TABLE IF EXISTS dws_traffic_session_page_view_1d;
CREATE EXTERNAL TABLE dws_traffic_session_page_view_1d
(
    `session_id`     STRING COMMENT '会话ID',
    `mid_id`         string comment '设备ID',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `version_code`   string comment 'APP版本号',
    `channel`        string comment '渠道',
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
    `page_count_1d`  BIGINT COMMENT '最近1日浏览页面数'
) COMMENT '流量域会话粒度页面浏览最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_session_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//每日装载

insert overwrite table dws_traffic_session_page_view_1d partition (dt)
select t1.session_id,
       t1.mid_id,
       t1.brand,
       t1.model,
       t1.operate_system,
       t1.version_code,
       t1.channel,
       t1.during_time,
       t1.num,
       t1.dt
from (select session_id,
             `mid_id`,
             `brand`,
             `model`,
             `operate_system`,
             `version_code`,
             `channel`,
             sum(during_time) over (partition by session_id)  during_time,
             count(session_id) over (partition by session_id) num,
             row_number() over (partition by session_id)      rn,
             dt
      from dwd_traffic_page_view_inc
      where dt = '2022-06-08') t1
where t1.rn = 1;

select common.sid,
       common.uid,
       page.item,
       page.during_time,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd') dt
from ods_log_inc
where common.sid = '031f4fe2-7247-412d-87a0-e10d5e206288';


//10.1.9 流量域访客页面粒度页面浏览最近1日汇总表

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
    LOCATION '/warehouse/gmall/dws/dws_traffic_page_visitor_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//每日装载

insert overwrite table dws_traffic_page_visitor_page_view_1d partition (dt)
select `mid_id`,
       `brand`,
       `model`,
       `operate_system`,
       page_id,
       sum(during_time)   during_time,
       count(mid_id)      num,
       collect_set(dt)[0] dt
from dwd_traffic_page_view_inc
where dt = '2022-06-08'
  and user_id is null
group by mid_id, brand, model, operate_system, page_id;


//10.2 最近n日汇总表


//10.2.1 交易域用户商品粒度订单最近n日汇总表

DROP TABLE IF EXISTS dws_trade_user_sku_order_nd;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_nd
(
    `user_id`                    STRING COMMENT '用户ID',
    `sku_id`                     STRING COMMENT 'SKU_ID',
    `sku_name`                   STRING COMMENT 'SKU名称',
    `category1_id`               STRING COMMENT '一级品类ID',
    `category1_name`             STRING COMMENT '一级品类名称',
    `category2_id`               STRING COMMENT '二级品类ID',
    `category2_name`             STRING COMMENT '二级品类名称',
    `category3_id`               STRING COMMENT '三级品类ID',
    `category3_name`             STRING COMMENT '三级品类名称',
    `tm_id`                      STRING COMMENT '品牌ID',
    `tm_name`                    STRING COMMENT '品牌名称',
    `order_count_7d`             STRING COMMENT '最近7日下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单件数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单件数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近n日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//测试

select
    `user_id`,
    `sku_id`      ,
    `sku_name`      ,
    `category1_id`  ,
    `category1_name`,
    `category2_id`  ,
    `category2_name`,
    `category3_id`  ,
    `category3_name`,
    `tm_id`       ,
    `tm_name`,
    sum(`if`(dt>date_sub('2022-06-08',7),order_count_1d,0)) order_count_7d
from dws_trade_user_sku_order_1d
where dt<='2022-06-08' and dt>date_sub('2022-06-08',30)
group by `user_id`,
    `sku_id`      ,
`sku_name`      ,
`category1_id`  ,
`category1_name`,
`category2_id`  ,
`category2_name`,
`category3_id`  ,
`category3_name`,
`tm_id`       ,
`tm_name`      ;

//每日装载
insert overwrite table dws_trade_user_sku_order_nd partition (dt = '2022-06-09')
select t1.`user_id`,-- STRING COMMENT '用户ID',
       t1.`sku_id`,--     STRING COMMENT 'SKU_ID',
       `sku_name`,--     STRING COMMENT 'SKU名称',
       `category1_id`,--    STRING COMMENT '一级品类ID',
       `category1_name`,--    STRING COMMENT '一级品类名称',
       `category2_id`,--    STRING COMMENT '二级品类ID',
       `category2_name`,--    STRING COMMENT '二级品类名称',
       `category3_id`,--    STRING COMMENT '三级品类ID',
       `category3_name`,--    STRING COMMENT '三级品类名称',
       `tm_id`,--     STRING COMMENT '品牌ID',
       `tm_name`,--     STRING COMMENT '品牌名称',
       `order_count_7d`,--    STRING COMMENT '最近7日下单次数',
       `order_num_7d`,--    BIGINT COMMENT '最近7日下单件数',
       `order_original_amount_7d`,--DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
       `activity_reduce_amount_7d`,--DECIMAL(16, 2) COMMENT '最近7日活动优惠金额',
       `coupon_reduce_amount_7d`,--DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额',
       `order_total_amount_7d`,--DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
       `order_count_30d`,--BIGINT COMMENT '最近30日下单次数',
       `order_num_30d`,--BIGINT COMMENT '最近30日下单件数',
       `order_original_amount_30d`,--DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
       `activity_reduce_amount_30d`,--DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',
       `coupon_reduce_amount_30d`,--DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',
       `order_total_amount_30d` --DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
from (select user_id,
             sku_id,
             sum(order_count_1d)            order_count_7d,
             sum(order_num_1d)              order_num_7d,
             sum(order_original_amount_1d)  order_original_amount_7d,
             sum(activity_reduce_amount_1d) activity_reduce_amount_7d,
             sum(coupon_reduce_amount_1d)   coupon_reduce_amount_7d,
             sum(order_total_amount_1d)     order_total_amount_7d
      from dws_trade_user_sku_order_1d
      where dt between date_sub('2022-06-09', 6) and '2022-06-09'
      group by user_id, sku_id) t1
         join
     (select user_id,
             sku_id,
             sku_name,
             collect_set(category1_id)[0]   category1_id,
             collect_set(category1_name)[0] category1_name,
             collect_set(category2_id)[0]   category2_id,
             collect_set(category2_name)[0] category2_name,
             collect_set(category3_id)[0]   category3_id,
             collect_set(category3_name)[0] category3_name,
             collect_set(tm_id)[0]          tm_id,
             collect_set(tm_name)[0]        tm_name,
             sum(order_count_1d)            order_count_30d,
             sum(order_num_1d)              order_num_30d,
             sum(order_original_amount_1d)  order_original_amount_30d,
             sum(activity_reduce_amount_1d) activity_reduce_amount_30d,
             sum(coupon_reduce_amount_1d)   coupon_reduce_amount_30d,
             sum(order_total_amount_1d)     order_total_amount_30d
      from dws_trade_user_sku_order_1d
      where dt between date_sub('2022-06-09', 29) and '2022-06-09'
      group by user_id, sku_id,sku_name) t2 on t1.user_id = t2.user_id and t1.sku_id = t2.sku_id;




//10.2.2 交易域省份粒度订单最近n日汇总表

DROP TABLE IF EXISTS dws_trade_province_order_nd;
CREATE EXTERNAL TABLE dws_trade_province_order_nd
(
    `province_id`                STRING COMMENT '省份ID',
    `province_name`              STRING COMMENT '省份名称',
    `area_code`                  STRING COMMENT '地区编码',
    `iso_code`                   STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                 STRING COMMENT '新版国际标准地区编码',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_province_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//每日装载

insert overwrite table dws_trade_province_order_nd partition (dt = '2022-06-09')
select t1.`province_id`,-- STRING COMMENT '省份ID',
       `province_name`,--     STRING COMMENT '省份名称',
       `area_code`,--     STRING COMMENT '地区编码',
       `iso_code`,--     STRING COMMENT '旧版国际标准地区编码',
       `iso_3166_2`,--     STRING COMMENT '新版国际标准地区编码',
       `order_count_7d`,--     BIGINT COMMENT '最近7日下单次数',
       `order_original_amount_7d`,-- DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
       `activity_reduce_amount_7d`,-- DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
       `coupon_reduce_amount_7d`,-- DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
       `order_total_amount_7d`,-- DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
       `order_count_30d`,-- BIGINT COMMENT '最近30日下单次数',
       `order_original_amount_30d`,-- DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
       `activity_reduce_amount_30d`,-- DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
       `coupon_reduce_amount_30d`,-- DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
       `order_total_amount_30d` -- DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
from (select province_id,
             collect_set(province_name)[0]  `province_name`,
             collect_set(area_code)[0]      `area_code`,
             collect_set(iso_code)[0]       `iso_code`,
             collect_set(iso_3166_2)[0]     `iso_3166_2`,
             sum(order_count_1d)            order_count_7d,
             sum(order_original_amount_1d)  order_original_amount_7d,
             sum(activity_reduce_amount_1d) activity_reduce_amount_7d,
             sum(coupon_reduce_amount_1d)   coupon_reduce_amount_7d,
             sum(order_total_amount_1d)     order_total_amount_7d
      from dws_trade_province_order_1d
      where dt between date_sub('2022-06-09', 6) and '2022-06-09'
      group by province_id) t1
         join
     (select province_id,
             sum(order_count_1d)            order_count_30d,
             sum(order_original_amount_1d)  order_original_amount_30d,
             sum(activity_reduce_amount_1d) activity_reduce_amount_30d,
             sum(coupon_reduce_amount_1d)   coupon_reduce_amount_30d,
             sum(order_total_amount_1d)     order_total_amount_30d
      from dws_trade_province_order_1d
      where dt between date_sub('2022-06-09', 29) and '2022-06-09'
      group by province_id) t2 on t1.province_id = t2.province_id;

select *
from dws_trade_province_order_nd
where province_id = '3'


//10.3 历史至今汇总表

DROP TABLE IF EXISTS dws_trade_user_order_td;
CREATE EXTERNAL TABLE dws_trade_user_order_td
(
    `user_id`                   STRING COMMENT '用户ID',
    `order_date_first`          STRING COMMENT '历史至今首次下单日期',
    `order_date_last`           STRING COMMENT '历史至今末次下单日期',
    `order_count_td`            BIGINT COMMENT '历史至今下单次数',
    `order_num_td`              BIGINT COMMENT '历史至今购买商品件数',
    `original_amount_td`        DECIMAL(16, 2) COMMENT '历史至今下单原始金额',
    `activity_reduce_amount_td` DECIMAL(16, 2) COMMENT '历史至今下单活动优惠金额',
    `coupon_reduce_amount_td`   DECIMAL(16, 2) COMMENT '历史至今下单优惠券优惠金额',
    `total_amount_td`           DECIMAL(16, 2) COMMENT '历史至今下单最终金额'
) COMMENT '交易域用户粒度订单历史至今汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//每日装载

insert overwrite table dws_trade_user_order_td partition (dt = '2022-06-08')
select t1.user_id,
       order_date_first,
       order_date_last,
       order_count_td,
       order_num_td,
       original_amount_td,
       activity_reduce_amount_td,
       coupon_reduce_amount_td,
       total_amount_td
from (select user_id,
             first_value(dt, false) over (partition by user_id)         order_date_first,
             last_value(dt, false) over (partition by user_id)          order_date_last,
             sum(order_count_1d) over (partition by user_id)            order_count_td,
             sum(order_num_1d) over (partition by user_id)              order_num_td,
             sum(order_original_amount_1d) over (partition by user_id)  original_amount_td,
             sum(activity_reduce_amount_1d) over (partition by user_id) activity_reduce_amount_td,
             sum(coupon_reduce_amount_1d) over (partition by user_id)   coupon_reduce_amount_td,
             sum(order_total_amount_1d) over (partition by user_id)     total_amount_td,
             row_number() over (partition by user_id)                   rn
      from dws_trade_user_order_1d
      where dt <= '2022-06-08') t1
where t1.rn = 1;


//每日
insert overwrite table dws_trade_user_order_td partition (dt = '2022-06-09')
select nvl(new.user_id, old.user_id),
       nvl(old.order_date_first, new.dt),
       nvl(new.dt, order_date_last),
       nvl(old.order_count_td, 0) + nvl(new.order_count_1d, 0),
       nvl(old.order_num_td, 0) + nvl(new.order_num_1d, 0),
       nvl(old.original_amount_td, 0) + nvl(new.order_original_amount_1d, 0),
       nvl(old.activity_reduce_amount_td, 0) + nvl(new.activity_reduce_amount_1d, 0),
       nvl(old.coupon_reduce_amount_td, 0) + nvl(new.coupon_reduce_amount_1d, 0),
       nvl(old.total_amount_td, 0) + nvl(new.order_total_amount_1d, 0)
from (select user_id,
             order_date_first,
             order_date_last,
             order_count_td,
             order_num_td,
             original_amount_td,
             activity_reduce_amount_td,
             coupon_reduce_amount_td,
             total_amount_td
      from dws_trade_user_order_td
      where dt = date_sub('2022-06-08', 1)) old
         full join
     (select user_id,
             order_count_1d,
             order_num_1d,
             order_original_amount_1d,
             activity_reduce_amount_1d,
             coupon_reduce_amount_1d,
             order_total_amount_1d,
             dt
      from dws_trade_user_order_1d
      where dt = '2022-06-09') new
     on new.user_id = old.user_id;


//10.3.2 用户域用户粒度登录历史至今汇总表

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
    LOCATION '/warehouse/gmall/dws/dws_user_user_login_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//每日装载
insert overwrite table dws_user_user_login_td partition (dt = '2022-06-04')
select user_id,
       max(login_time),
       min(login_time),
       count(user_id)
from dwd_user_login_inc
where dt <= '2022-06-04'
group by user_id;


insert overwrite table dws_user_user_login_td partition (dt = '2022-06-04')
select t1.user_id,
       nvl(t2.last, t1.date_id) last,
       t1.date_id,
       nvl(t2.num, 1)
from (select user_id,
             date_id
      from dwd_user_register_inc
      where dt = '2022-06-04') t1
         left join (select user_id,
                           max(date_format(login_time, 'yyyy-MM-dd')) last,
                           count(*)                                   num
                    from dwd_user_login_inc
                    where dt = '2022-06-04'
                    group by user_id) t2 on t1.user_id = t2.user_id;


//每日数据

insert overwrite table dws_user_user_login_td partition (dt = '2022-06-09')
select nvl(new.user_id, old.user_id),
       nvl(new.last, old.login_date_last),
       nvl(date_id, login_date_first),
       nvl(login_count_td, 0) + nvl(num, 0)
from (select user_id, login_date_last, login_date_first, login_count_td
      from dws_user_user_login_td
      where date_add(dt,1)='2022-06-09') old
         full join (select nvl(t1.user_id,t2.user_id) user_id,
                           nvl(t2.last, t1.date_id) last,
                           t1.date_id,
                           nvl(t2.num, 1)           num
                    from (select user_id,
                                 date_id
                          from dwd_user_register_inc
                          where dt = '2022-06-09') t1
                             full join (select user_id,
                                               max(date_format(login_time, 'yyyy-MM-dd')) last,
                                               count(*)                                   num
                                        from dwd_user_login_inc
                                        where dt = '2022-06-09'
                                        group by user_id) t2 on t1.user_id = t2.user_id) new
                   on new.user_id = old.user_id;



select *
from (select user_id, login_date_last, login_date_first, login_count_td
      from dws_user_user_login_td
      where dt = '2022-06-04') old
         full join (select nvl(t1.user_id,t2.user_id) user_id,
                           nvl(t2.last, t1.date_id) last,
                           t1.date_id,
                           nvl(t2.num, 1)           num
                    from (select user_id,
                                 date_id
                          from dwd_user_register_inc
                          where dt = '2022-06-05') t1
                             full join (select user_id,
                                               max(date_format(login_time, 'yyyy-MM-dd')) last,
                                               count(*)                                   num
                                        from dwd_user_login_inc
                                        where dt = '2022-06-05'
                                        group by user_id) t2 on t1.user_id = t2.user_id) new
                   on new.user_id = old.user_id;
