//9.1 交易域加购事务事实表
DROP TABLE IF EXISTS dwd_trade_cart_add_inc;
create external table dwd_trade_cart_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '加购时间',
    `sku_num`     BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_add_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

//首日装载
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') data_id,
       data.create_time,
       data.sku_num,
       date_format(data.create_time, 'yyyy-MM-dd')
from ods_cart_info_inc
where type = 'bootstrap-insert'
  and dt = '2022-06-08';


select sku_id, sku_num, date_id
from dwd_trade_cart_add_inc
where user_id = 31;

//每日装载
insert overwrite table dwd_trade_cart_add_inc partition (dt = '2022-06-09')
select data.id,
       data.user_id,
       data.sku_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') create_time,
       if(type = 'insert', data.sku_num, data.sku_num - old['sku_num'])           sku_num
from ods_cart_info_inc
where dt = '2022-06-09'
  and (type = 'insert'
    or (type = 'update' and old['sku_num'] is not null and data.sku_num > cast(old['sku_num'] as bigint)));

set hive.strict.checks.type.safety=true;

select data.id,
       data.user_id,
       data.sku_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') create_time,
       `if`(type = 'insert', data.sku_num, data.sku_num - cast(old['sku_num'] as bigint))
from ods_cart_info_inc
where dt = '2022-06-09'
  and (type = 'insert' or
       (type = 'update' and old['sku_num'] is not null and data.sku_num > cast(old['sku_num'] as bigint)));


//9.2 交易域下单事务事实表
DROP TABLE IF EXISTS dwd_trade_order_detail_inc;

create external table dwd_trade_order_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING comment '订单id',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT 'SKU_ID',
    `date_id`               STRING COMMENT '日期ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING comment '参与活动id',
    `activity_rule_id`      STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `create_time`           STRING COMMENT '下单时间',
    `sku_num`               BIGINT COMMENT '下单件数',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;

//首日装载
with order_info as (select data.id,
                           data.user_id,
                           data.province_id
                    from ods_order_info_inc
                    where dt = '2022-06-08'
                      and type = 'bootstrap-insert'),
     order_detail as (select data.id,
                             data.order_id,
                             data.sku_id,
                             data.sku_num,
                             data.create_time,
                             data.sku_num * ods_order_detail_inc.data.order_price split_original_amount,
                             data.split_total_amount,
                             data.split_activity_amount,
                             data.split_coupon_amount
                      from ods_order_detail_inc
                      where dt = '2022-06-09'
                        and type = 'bootstrap-insert'),
     order_detail_activity as (select data.order_detail_id,
                                      data.activity_id,
                                      data.activity_rule_id
                               from ods_order_detail_activity_inc
                               where dt = '2022-06-08'
                                 and type = 'bootstrap-insert'),
     order_detail_coupon as (select data.order_detail_id,
                                    data.coupon_id
                             from ods_order_detail_coupon_inc
                             where dt = '2022-06-08'
                               and type = 'bootstrap-insert')
insert
overwrite
table
dwd_trade_order_detail_inc
partition
(
dt
)
select order_detail.id,
       order_detail.order_id,
       order_info.user_id,
       order_detail.sku_id,
       date_format(order_detail.create_time, 'yyyy-MM-dd') data_id,
       order_info.province_id,
       order_detail_activity.activity_id,
       order_detail_activity.activity_rule_id,
       order_detail_coupon.coupon_id,
       order_detail.create_time,
       order_detail.sku_num,
       order_detail.split_original_amount,
       nvl(split_activity_amount, 0.0),
       nvl(split_coupon_amount, 0.0),
       split_total_amount,
       date_format(order_detail.create_time, 'yyyy-MM-dd')
from order_detail
         left join order_info
                   on order_detail.order_id = order_info.id
         left join order_detail_activity on order_detail.id = order_detail_activity.order_detail_id
         left join order_detail_coupon on order_detail_coupon.order_detail_id = order_detail.id;

alter table ods_order_detail_inc
    add partition (dt = '2022-06-08');

insert overwrite table ods_order_detail_inc partition (dt)
select type,
       ts,
       data,
       old,
       date_sub(dt, 1)
from ods_order_detail_inc
where dt = '2022-06-09';

alter table ods_order_detail_inc
    drop partition (dt = '2022-06-09');


//每日装载
with order_info as (select data.id,
                           data.user_id,
                           data.province_id
                    from ods_order_info_inc
                    where dt = '2022-06-09'
                      and type = 'insert'),
     order_detail as (select data.id,
                             data.order_id,
                             data.sku_id,
                             data.sku_num,
                             data.create_time,
                             data.sku_num * ods_order_detail_inc.data.order_price split_original_amount,
                             data.split_total_amount,
                             data.split_activity_amount,
                             data.split_coupon_amount
                      from ods_order_detail_inc
                      where dt = '2022-06-09'
                        and type = 'insert'),
     order_detail_activity as (select data.order_detail_id,
                                      data.activity_id,
                                      data.activity_rule_id
                               from ods_order_detail_activity_inc
                               where dt = '2022-06-09'
                                 and type = 'insert'),
     order_detail_coupon as (select data.order_detail_id,
                                    data.coupon_id
                             from ods_order_detail_coupon_inc
                             where dt = '2022-06-09'
                               and type = 'insert')
insert
overwrite
table
dwd_trade_order_detail_inc
partition
(
dt
)
select order_detail.id,
       order_detail.order_id,
       order_info.user_id,
       order_detail.sku_id,
       date_format(order_detail.create_time, 'yyyy-MM-dd') data_id,
       order_info.province_id,
       order_detail_activity.activity_id,
       order_detail_activity.activity_rule_id,
       order_detail_coupon.coupon_id,
       order_detail.create_time,
       order_detail.sku_num,
       order_detail.split_original_amount,
       nvl(split_activity_amount, 0.0),
       nvl(split_coupon_amount, 0.0),
       split_total_amount,
       date_format(order_detail.create_time, 'yyyy-MM-dd')
from order_detail
         left join order_info
                   on order_detail.order_id = order_info.id
         left join order_detail_activity on order_detail.id = order_detail_activity.order_detail_id
         left join order_detail_coupon on order_detail_coupon.order_detail_id = order_detail.id;

//9.3 交易域支付成功事务事实表

DROP TABLE IF EXISTS dwd_trade_pay_detail_suc_inc;
CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT 'SKU_ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`      STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`               STRING COMMENT '支付日期ID',
    `callback_time`         STRING COMMENT '支付成功时间',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域支付成功事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//首日装载

with order_detail as (select data.id,
                             data.order_id,
                             data.sku_id,
                             data.sku_num,
                             data.sku_num * ods_order_detail_inc.data.order_price split_original_amount,
                             data.split_total_amount,
                             data.split_activity_amount,
                             data.split_coupon_amount
                      from ods_order_detail_inc
                      where dt = '2022-06-08'
                        and type = 'bootstrap-insert'),
     order_info as (select data.id,
                           data.province_id
                    from ods_order_info_inc
                    where dt = '2022-06-08'
                      and type = 'bootstrap-insert'),
     activity as (select data.order_detail_id,
                         data.activity_id,
                         data.activity_rule_id
                  from ods_order_detail_activity_inc
                  where dt = '2022-06-08'
                    and type = 'bootstrap-insert'),
     coupon as (select data.order_detail_id,
                       data.coupon_id
                from ods_order_detail_coupon_inc
                where dt = '2022-06-08'
                  and type = 'bootstrap-insert'),
     payment as (select data.order_id,
                        data.user_id,
                        data.payment_type,
                        data.callback_time
                 from ods_payment_info_inc
                 where dt = '2022-06-08'
                   and type = 'bootstrap-insert'
                   and data.payment_status = '1602'),
     dic as (select dic_code,
                    dic_name
             from ods_base_dic_full
             where parent_code = '11'
               and dt = '2022-06-08')
insert
overwrite
table
dwd_trade_pay_detail_suc_inc
partition
(
dt
)
select order_detail.id,
       order_detail.order_id,
       payment.user_id,
       order_detail.sku_id,
       order_info.province_id,
       activity.activity_id,
       activity.activity_rule_id,
       coupon.coupon_id,
       payment.payment_type,
       dic.dic_name,
       date_format(payment.callback_time, 'yyyy-MM-dd') date_id,
       payment.callback_time,
       order_detail.sku_num,
       order_detail.split_original_amount,
       nvl(order_detail.split_activity_amount, 0.0),
       nvl(split_coupon_amount, 0.0),
       order_detail.split_total_amount,
       date_format(payment.callback_time, 'yyyy-MM-dd')
from payment
         left join order_detail on payment.order_id = order_detail.order_id
         left join order_info on order_detail.order_id = order_info.id
         left join activity on activity.order_detail_id = order_detail.order_id
         left join coupon on order_detail.order_id = coupon.order_detail_id
         left join dic on dic.dic_code = payment.payment_type;


//每日装载
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt = '2022-06-09')
select od.id,
       od.order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       payment_type,
       pay_dic.dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       sku_num,
       split_original_amount,
       nvl(split_activity_amount, 0.0),
       nvl(split_coupon_amount, 0.0),
       split_total_amount
from (select data.id,
             data.order_id,
             data.sku_id,
             data.sku_num,
             data.sku_num * data.order_price split_original_amount,
             data.split_total_amount,
             data.split_activity_amount,
             data.split_coupon_amount
      from ods_order_detail_inc
      where (dt = '2022-06-09' or dt = date_add('2022-06-09', -1))
        and (type = 'insert' or type = 'bootstrap-insert')) od
         join
     (select data.user_id,
             data.order_id,
             data.payment_type,
             data.callback_time
      from ods_payment_info_inc
      where dt = '2022-06-09'
        and type = 'update'
        and array_contains(map_keys(old), 'pyment_status')
        and data.payment_status = '1602')
     on od.order_id = pi.order_id
         left join
     (select data.id,
             data.province_id
      from ods_order_info_inc
      where (dt = '2022-06-09' or dt = date_add('2022-06-09', -1))
        and (type = 'insert' or type = 'bootstrap-insert')) oi
     on od.order_id = oi.id
         left join
     (select data.order_detail_id,
             data.activity_id,
             data.activity_rule_id
      from ods_order_detail_activity_inc
      where (dt = '2022-06-09' or dt = date_add('2022-06-09', -1))
        and (type = 'insert' or type = 'bootstrap-insert')) act
     on od.id = act.order_detail_id
         left join
     (select data.order_detail_id,
             data.coupon_id
      from ods_order_detail_coupon_inc
      where (dt = '2022-06-09' or dt = date_add('2022-06-09', -1))
        and (type = 'insert' or type = 'bootstrap-insert')) cou
     on od.id = cou.order_detail_id
         left join
     (select dic_code,
             dic_name
      from ods_base_dic_full
      where dt = '2022-06-09'
        and parent_code = '11') pay_dic
     on pi.payment_type = pay_dic.dic_code;


//9.4 交易域购物车周期快照事实表

DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`       STRING COMMENT '编号',
    `user_id`  STRING COMMENT '用户ID',
    `sku_id`   STRING COMMENT 'SKU_ID',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num`  BIGINT COMMENT '现存商品件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

//每日数据
insert overwrite table dwd_trade_cart_full partition (dt = '2022-06-09')
select *
from (select id,
             user_id,
             sku_id,
             sku_name,
             sku_num
      from ods_cart_info_full
      where dt = '2022-06-09'
        and is_ordered = '0') cart_info;

select user_id,
       sku_id,
       sku_name,
       sku_num,
       is_ordered,
       dt
from ods_cart_info_full
where user_id = '41';


//9.5 交易域交易流程累积快照事实表

DROP TABLE IF EXISTS dwd_trade_trade_flow_acc;
CREATE EXTERNAL TABLE dwd_trade_trade_flow_acc
(
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `province_id`           STRING COMMENT '省份ID',
    `order_date_id`         STRING COMMENT '下单日期ID',
    `order_time`            STRING COMMENT '下单时间',
    `payment_date_id`       STRING COMMENT '支付日期ID',
    `payment_time`          STRING COMMENT '支付时间',
    `finish_date_id`        STRING COMMENT '确认收货日期ID',
    `finish_time`           STRING COMMENT '确认收货时间',
    `order_original_amount` DECIMAL(16, 2) COMMENT '下单原始价格',
    `order_activity_amount` DECIMAL(16, 2) COMMENT '下单活动优惠分摊',
    `order_coupon_amount`   DECIMAL(16, 2) COMMENT '下单优惠券优惠分摊',
    `order_total_amount`    DECIMAL(16, 2) COMMENT '下单最终价格分摊',
    `payment_amount`        DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域交易流程累积快照事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_trade_flow_acc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

//每日数据
with oi as (select data.id,
                   data.user_id,
                   data.province_id,
                   data.create_time,
                   data.original_total_amount,
                   data.activity_reduce_amount,
                   data.coupon_reduce_amount,
                   data.total_amount
            from ods_order_info_inc
            where dt = '2022-06-08'
              and type = 'bootstrap-insert'),
     pi as (select data.order_id,
                   data.callback_time,
                   data.total_amount payment_amount
            from ods_payment_info_inc
            where dt = '2022-06-08'
              and type = 'bootstrap-insert'
              and data.payment_status = '1602'),
     osl as (select data.order_id,
                    data.create_time
             from ods_order_status_log_inc
             where data.order_status = '1004'
               and dt = '2022-06-08'
               and type = 'bootstrap-insert')
insert
overwrite
table
dwd_trade_trade_flow_acc
partition
(
dt
)
select oi.order_id,
       oi.user_id,
       oi.province_id,
       date_format(oi.create_time, 'yyyy-MM-dd')   order_date_id,
       oi.create_time,
       date_format(pi.callback_time, 'yyyy-MM-dd') payment_date_id,
       pi.callback_time,
       date_format(osl.create_time, 'yyyy-MM-dd')  finish_date_id,
       osl.create_time,
       original_total_amount,
       activity_reduce_amount,
       coupon_reduce_amount,
       total_amount,
       nvl(pi.payment_amount, 0.0),
       nvl(date_format(osl.create_time, 'yyyy-MM-dd'), '9999-12-31')
from oi
         left join pi on pi.order_id = oi.id
         left join osl on osl.order_id = oi.id;

set hive.exec.dynamic.partition.mode=nonstrict;


//每日装载
insert overwrite table dwd_trade_trade_flow_acc partition (dt)
select oi.order_id,
       user_id,
       province_id,
       order_date_id,
       order_time,
       nvl(oi.payment_date_id, pi.payment_date_id),
       nvl(oi.payment_time, pi.payment_time),
       nvl(oi.finish_date_id, log.finish_date_id),
       nvl(oi.finish_time, log.finish_time),
       order_original_amount,
       order_activity_amount,
       order_coupon_amount,
       order_total_amount,
       nvl(oi.payment_amount, pi.payment_amount),
       nvl(nvl(oi.finish_time, log.finish_time), '9999-12-31')
from (select order_id,
             user_id,
             province_id,
             order_date_id,
             order_time,
             payment_date_id,
             payment_time,
             finish_date_id,
             finish_time,
             order_original_amount,
             order_activity_amount,
             order_coupon_amount,
             order_total_amount,
             payment_amount
      from dwd_trade_trade_flow_acc
      where dt = '9999-12-31'
      union all
      select data.id,
             data.user_id,
             data.province_id,
             date_format(data.create_time, 'yyyy-MM-dd') order_date_id,
             data.create_time,
             null                                        payment_date_id,
             null                                        payment_time,
             null                                        finish_date_id,
             null                                        finish_time,
             data.original_total_amount,
             data.activity_reduce_amount,
             data.coupon_reduce_amount,
             data.total_amount,
             null                                        payment_amount
      from ods_order_info_inc
      where type = 'insert'
        and dt = '2022-06-09') oi
         left join
     (select data.order_id,
             date_format(data.callback_time, 'yyyy-MM-dd') payment_date_id,
             data.callback_time                            payment_time,
             data.total_amount                             payment_amount
      from ods_payment_info_inc
      where type = 'update'
        and data.payment_status = '1602'
        and array_contains(map_keys(old), 'payment_status')) pi on oi.order_id = pi.order_id
         join (select data.order_id,
                      date_format(data.create_time, 'yyyy-MM-dd') finish_date_id,
                      data.create_time                            finish_time
               from ods_order_status_log_inc
               where type = 'insert'
                 and data.order_status = '1004') log on oi.order_id = log.order_id;

select *
from (select data.id,
             data.order_status,
             data.create_time
      from ods_order_info_inc
      where dt = '2022-06-09'
        and type = 'update') oi
         join (select data.order_id,
                      data.payment_status,
                      data.callback_time
               from ods_payment_info_inc
               where dt = '2022-06-09'
                 and type = 'update') pi on oi.id = pi.order_id;

//9.6 工具域优惠券使用(支付)事务事实表

DROP TABLE IF EXISTS dwd_tool_coupon_used_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_used_inc
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT '用户ID',
    `order_id`     STRING COMMENT '订单ID',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用(支付)时间'
) COMMENT '优惠券使用（支付）事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_used_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");


//首日装载
insert overwrite table dwd_tool_coupon_used_inc partition (dt)
select *
from (select data.id,
             data.coupon_id,
             data.user_id,
             data.order_id,
             date_format(data.used_time, 'yyyy-MM-dd') date_id,
             data.used_time,
             date_format(data.used_time, 'yyyy-MM-dd')
      from ods_coupon_use_inc
      where type = 'bootstrap-insert'
        and data.coupon_status = '1403'
        and dt = '2022-06-08') cu;

select *,
       data.coupon_status
from ods_coupon_use_inc
where old is not null;

//每日装载
insert overwrite table dwd_tool_coupon_used_inc partition (dt = '2022-06-09')
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.used_time, 'yyyy-MM-dd') date_id,
       data.used_time
from ods_coupon_use_inc
where dt = '2022-06-09'
  and type = 'update'
  and array_contains(map_keys(old), 'used_time');

//9.7 互动域收藏商品事务事实表

DROP TABLE IF EXISTS dwd_interaction_favor_add_inc;
CREATE EXTERNAL TABLE dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '互动域收藏商品事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_interaction_favor_add_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

//首日装载
insert overwrite table dwd_interaction_favor_add_inc partition (dt)
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       date_format(data.create_time, 'yyyy-MM-dd')
from ods_favor_info_inc
where type = 'bootstrap-insert';

//每日加载
insert overwrite table dwd_interaction_favor_add_inc partition (dt = '2022-06-09')
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time
from ods_favor_info_inc
where type = 'insert'
  and dt = '2022-06-09';


select data.user_id,
       data.sku_id,
       data.is_cancel,
       data.create_time
from ods_favor_info_inc
where data.user_id = '168'
  and ((dt = '2022-06-08' and type = 'bootstrap-insert') or (dt = '2022-06-09' and type = 'insert'));


//9.8 流量域页面浏览事务事实表

DROP TABLE IF EXISTS dwd_traffic_page_view_inc;
CREATE EXTERNAL TABLE dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份ID',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备ID',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员ID',
    `version_code`   STRING COMMENT 'APP版本号',
    `page_item`      STRING COMMENT '目标ID',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页ID',
    `page_id`        STRING COMMENT '页面ID ',
    `from_pos_id`    STRING COMMENT '点击坑位ID',
    `from_pos_seq`   STRING COMMENT '点击坑位位置',
    `refer_id`       STRING COMMENT '营销渠道ID',
    `date_id`        STRING COMMENT '日期ID',
    `view_time`      STRING COMMENT '跳入时间',
    `session_id`     STRING COMMENT '所属会话ID',
    `during_time`    BIGINT COMMENT '持续时间毫秒'
) COMMENT '流量域页面浏览事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//每日装载
insert overwrite table dwd_traffic_page_view_inc partition (dt = '2022-06-08');
select common.ar,
       common.ba,
       common.ch,
       common.is_new,
       common.md,
       common.mid,
       common.os,
       common.uid,
       common.vc,
       page.item,
       page.item_type,
       page.last_page_id,
       page.page_id,
       page.from_pos_id,
       page.from_pos_seq,
       page.refer_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') view_time,
       common.sid,
       page.during_time
from ods_log_inc
where dt = '2022-06-08'
  and page is not null;


//9.9 用户域用户注册事务事实表

DROP TABLE IF EXISTS dwd_user_register_inc;
CREATE EXTERNAL TABLE dwd_user_register_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `create_time`    STRING COMMENT '注册时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备ID',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_register_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

//每日装载
insert overwrite table dwd_user_register_inc partition (dt);
select ui.id,
       date_format(create_time, 'yyyy-MM-dd'),
       create_time,
       ch,
       ar,
       vc,
       mid,
       ba,
       md,
       os,
       date_format(create_time, 'yyyy-MM-dd')
from (select data.id,
             data.create_time
      from ods_user_info_inc
      where dt = '2022-06-09'
        and type = 'insert') ui
         left join (select common.ar,
                           common.ba,
                           common.ch,
                           common.is_new,
                           common.md,
                           common.mid,
                           common.os,
                           common.uid,
                           common.vc,
                           row_number() over (partition by common.uid) rn
                    from ods_log_inc
                    where dt = '2022-06-09'
                      and page.page_id = 'register') li on ui.id = li.uid
where rn = 1
   or rn is null;

select *
from ods_log_inc
where common.uid = '22';

show partitions dwd_user_login_inc;
msck repair table dwd_user_login_inc sync partitions;

//9.10 用户域用户登录事务事实表
DROP TABLE IF EXISTS dwd_user_login_inc;
CREATE EXTERNAL TABLE dwd_user_login_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备ID',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户登录事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_login_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");


//首日装载

insert overwrite table dwd_user_login_inc partition (dt = '2022-06-09')
select uid,
       date_id,
       view_time,
       ch,
       ar,
       vc,
       mid,
       ba,
       md,
       os
from (select common.uid,
             date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
             date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') view_time,
             common.ch,
             common.ar,
             common.vc,
             common.mid,
             common.ba,
             common.md,
             common.os,
             row_number() over (partition by common.sid order by ts)             rn
      from ods_log_inc
      where page is not null
        and common.uid is not null
        and dt = '2022-06-09') li
where rn = 1;


select common.sid,
       common.uid
from ods_log_inc
order by sid;



select od.`id`,--  STRING COMMENT '编号',
       oi.id `order_id`,-- STRING COMMENT '订单ID',
       `user_id`,-- STRING COMMENT '用户ID',
       `sku_id`,-- STRING COMMENT 'SKU_ID',
       `province_id`,--STRING COMMENT '省份ID',
       `activity_id`,--STRING COMMENT '参与活动ID',
       `activity_rule_id`,--STRING COMMENT '参与活动规则ID',
       `coupon_id`,-- STRING COMMENT '使用优惠券ID',
       `payment_type_code`,--STRING COMMENT '支付类型编码',
       `payment_type_name`,--STRING COMMENT '支付类型名称',
       date_format(callback_time, 'yyyy-MM-dd'),-- STRING COMMENT '支付日期ID',
       `callback_time`,--STRING COMMENT '支付成功时间',
       `sku_num`,--  BIGINT COMMENT '商品数量',
       `split_original_amount`,--DECIMAL(16, 2) COMMENT '应支付原始金额',
       nvl(`split_activity_amount`, 0.0),--DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
       nvl(`split_coupon_amount`, 0.0),--DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
       `split_payment_amount` --DECIMAL(16, 2) COMMENT '支付金额'
from (select data.`id`,--   STRING COMMENT '编号',
             data.`order_id`,--  STRING COMMENT '订单ID',
             --data.`user_id`              ,--  STRING COMMENT '用户ID',
             data.`sku_id`,--  STRING COMMENT 'SKU_ID',
             --data.`province_id`          ,-- STRING COMMENT '省份ID',
             --data.`activity_id`          ,-- STRING COMMENT '参与活动ID',
             --data.`activity_rule_id`     ,-- STRING COMMENT '参与活动规则ID',
             --data.`coupon_id`            ,--  STRING COMMENT '使用优惠券ID',
             --data.`payment_type_code`    ,-- STRING COMMENT '支付类型编码',
             --data.`payment_type_name`    ,-- STRING COMMENT '支付类型名称',
             --data.`date_id`              ,--  STRING COMMENT '支付日期ID',
             --data.`callback_time`        ,-- STRING COMMENT '支付成功时间',
             data.`sku_num`,--   BIGINT COMMENT '商品数量',
             data.order_price * data.sku_num `split_original_amount`,-- DECIMAL(16, 2) COMMENT '应支付原始金额',
             data.`split_activity_amount`,-- DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
             data.`split_coupon_amount` -- DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
             --data.`split_payment_amount` -- DECIMAL(16, 2) COMMENT '支付金额'
      from ods_order_detail_inc
      where (dt = '2022-06-09' or dt = date_sub('2022-06-09', 1))
        and (type = 'insert' or type = 'bootstrap-insert')) od
         join (select data.order_id,
                      data.payment_type payment_type_code,
                      data.total_amount split_payment_amount,
                      data.callback_time
               from ods_payment_info_inc
               where dt = '2022-06-09'
                 and type = 'update'
                 and array_contains(map_keys(old), 'payment_status')
                 and data.payment_status = '1602') pay on od.order_id = pay.order_id
         left join (select dic_code,
                           dic_name payment_type_name
                    from ods_base_dic_full
                    where dt = '2022-06-09'
                      and parent_code = '11') dic on pay.payment_type_code = dic.dic_code
         left join (select data.id,
                           data.user_id,
                           data.province_id
                    from ods_order_info_inc
                    where (dt = '2022-06-09' or dt = date_sub('2022-06-09', 1))
                      and (type = 'insert' or type = 'bootstrap-insert')) oi on od.order_id = oi.id
         left join (select data.order_detail_id,
                           data.activity_id,
                           data.activity_rule_id
                    from ods_order_detail_activity_inc
                    where (dt = '2022-06-09' or dt = date_sub('2022-06-09', 1))
                      and (type = 'insert' or type = 'bootstrap-insert')) act on od.id = act.order_detail_id
         left join (select data.order_detail_id,
                           data.coupon_id
                    from ods_order_detail_coupon_inc
                    where (dt = '2022-06-09' or dt = date_sub('2022-06-09', 1))
                      and (type = 'insert' or type = 'bootstrap-insert')) cp on od.id = cp.order_detail_id