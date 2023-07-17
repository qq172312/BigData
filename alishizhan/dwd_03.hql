//1、交易域加购事务事实表      (维度字段 用户id 时间id 课程id) 度量：一个课程id

-------------------------------------------------------------------------------------------------------------------
编号    用户        时间id        课程id       加购时间
id   user_id      date_id     course_id     create_time
-------------------------------------------------------------------------------------------------------------------
drop table if exists dwd_trade_cart_add_inc;
create external table dwd_trade_cart_add_inc(
                                                `id`                  STRING COMMENT '编号',
                                                `user_id`            STRING COMMENT '用户ID',
                                                `course_id`             STRING COMMENT '课程ID',
                                                session_id              string comment '会话ID',
                                                `date_id`            STRING COMMENT '日期ID',
                                                `create_time`        STRING COMMENT '加购时间',
                                                `cart_price`            BIGINT COMMENT '放入购物车时价格'
)comment "交易域加购事务事实表"
    partitioned by (dt string)
    stored as orc
    location '/warehouse/edu/dwd/dwd_trade_cart_add_inc/'
    tblproperties ('orc.compress' = 'snappy');


--首日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select
    data.`id`                 ,-- STRING COMMENT '编号',
    data.`user_id`            ,--STRING COMMENT '用户ID',
    data.`course_id`          ,--   STRING COMMENT '课程ID',
    data.`session_id`         ,
    date_format(data.`create_time`,"yyyy-MM-dd") `date_id`            ,--STRING COMMENT '日期ID',
    data.`create_time`        ,--STRING COMMENT '加购时间',
    data.`cart_price`         ,--   BIGINT COMMENT '放入购物车时价格'
    '2022-06-08'
from ods_cart_info_inc
where dt="2022-06-08" and type="bootstrap-insert";

--每日
insert overwrite table dwd_trade_cart_add_inc partition (dt="2022-06-09")
select data.`id`                 ,-- STRING COMMENT '编号',
       data.`user_id`            ,--STRING COMMENT '用户ID',
       data.`course_id`          ,--   STRING COMMENT '课程ID',
       data.session_id           ,
       date_format(from_utc_timestamp(ts*1000,"GMT+8"),"yyyy-MM-dd") date_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss')  create_time,
       `if`(type="insert",data.cart_price,cast(data.cart_price as DECIMAL(16,2))-cast(old["cart_price"] as DECIMAL(16,2))) `cart_price`
from ods_cart_info_inc
where dt="2022-06-09" and (type="insert" or (type="update" and old["cart_price"] is not null and
                                             cast(data.cart_price as DECIMAL(16,2))>cast(old["cart_price"] as DECIMAL(16,2))));


//2、交易域下单事务事实表      (维度字段 用户id 时间id 课程id 地区id) 度量：课程id

-------------------------------------------------------------------------------------------------------------------
编号     订单id          用户            课程id         省份id           下单时间        下单日期
id      order_id       user_id      course_id     province_id     create_time      date_id

价格             优惠金额             实际价格
origin_price     reduce_amount       actual_price
-------------------------------------------------------------------------------------------------------------------

drop table if exists dwd_trade_order_detail_inc;
create external table dwd_trade_order_detail_inc (
                                                     `id`                     STRING COMMENT '编号',
                                                     `order_id`              STRING COMMENT '订单ID',
                                                     `user_id`               STRING COMMENT '用户ID',
                                                     `course_id`                STRING COMMENT '课程ID',
                                                     `province_id`          STRING COMMENT '省份ID',
                                                     session_id              string comment '会话ID',
                                                     source_id              string comment '来源id',
                                                     source_name            string comment '来源名称',
                                                     `date_id`               STRING COMMENT '下单日期ID',
                                                     `create_time`           STRING COMMENT '下单时间',
                                                     origin_amount           DECIMAL(16, 2) COMMENT '原始价格',
                                                     coupon_reduce           DECIMAL(16, 2) COMMENT '优惠券减免金额',
                                                     final_amount           DECIMAL(16, 2) COMMENT '最终金额'
)COMMENT '交易域下单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


--首日
--首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select
    oodi.`id`                   ,--  STRING COMMENT '编号',
    `order_id`             ,-- STRING COMMENT '订单ID',
    `user_id`              ,-- STRING COMMENT '用户ID',
    `course_id`            ,--    STRING COMMENT '课程ID',
    `province_id`          ,--STRING COMMENT '省份ID',
    ooii.session_id             ,-- string comment '会话ID',
    sour        ,
    source_site,
    `date_id`              ,-- STRING COMMENT '下单日期ID',
    `create_time`          ,-- STRING COMMENT '下单时间',
    origin_amount          ,-- DECIMAL(16, 2) COMMENT '原始价格',
    coupon_reduce          ,-- DECIMAL(16, 2) COMMENT '优惠券减免金额',
    final_amount           ,--DECIMAL(16, 2) COMMENT '最终金额'
    date_id
from (
         select
             data.`id`                  ,--  STRING COMMENT '编号',
             data.`order_id`            ,-- STRING COMMENT '订单ID',
             data.`user_id`             ,-- STRING COMMENT '用户ID',
             data.`course_id`           ,--    STRING COMMENT '课程ID',
             date_format(data.`create_time`,"yyyy-MM-dd")  `date_id`             ,-- STRING COMMENT '下单日期ID',
             data.`create_time`         ,-- STRING COMMENT '下单时间',
             data.origin_amount         ,-- DECIMAL(16, 2) COMMENT '原始价格',
             data.coupon_reduce         ,-- DECIMAL(16, 2) COMMENT '优惠券减免金额',
             data.final_amount          --DECIMAL(16, 2) COMMENT '最终金额'
         from ods_order_detail_inc
         where dt="2022-06-08" and type="bootstrap-insert"
     )oodi
          join (
    select data.id,
           data.province_id,
           data.session_id
    from ods_order_info_inc
    where dt="2022-06-08" and type="bootstrap-insert"
)ooii on oodi.order_id=ooii.id
          join (
    select common.sid ses,
           common.sc sour
    from ods_log_inc
    where dt = '2022-06-08'
)log on ooii.session_id = log.ses
        join (
    select id,
           source_site
    from ods_base_source_full
    where dt = '2022-06-08'
)sou on log.sour = sou.id;





--每日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (dt = '2022-06-09')
select
    oodi.`id`                   ,--  STRING COMMENT '编号',
    `order_id`             ,-- STRING COMMENT '订单ID',
    `user_id`              ,-- STRING COMMENT '用户ID',
    `course_id`            ,--    STRING COMMENT '课程ID',
    `province_id`          ,--STRING COMMENT '省份ID',
    ooii.session_id             ,-- string comment '会话ID',
    sour        ,
    source_site,
    `date_id`              ,-- STRING COMMENT '下单日期ID',
    `create_time`          ,-- STRING COMMENT '下单时间',
    origin_amount          ,-- DECIMAL(16, 2) COMMENT '原始价格',
    coupon_reduce          ,-- DECIMAL(16, 2) COMMENT '优惠券减免金额',
    final_amount           --DECIMAL(16, 2) COMMENT '最终金额'
from (
         select
             data.`id`                  ,--  STRING COMMENT '编号',
             data.`order_id`            ,-- STRING COMMENT '订单ID',
             data.`user_id`             ,-- STRING COMMENT '用户ID',
             data.`course_id`           ,--    STRING COMMENT '课程ID',
             date_format(data.`create_time`,"yyyy-MM-dd")  `date_id`             ,-- STRING COMMENT '下单日期ID',
             data.`create_time`         ,-- STRING COMMENT '下单时间',
             data.origin_amount         ,-- DECIMAL(16, 2) COMMENT '原始价格',
             data.coupon_reduce         ,-- DECIMAL(16, 2) COMMENT '优惠券减免金额',
             data.final_amount          --DECIMAL(16, 2) COMMENT '最终金额'
         from ods_order_detail_inc
         where dt="2022-06-09" and type="insert"
     )oodi
         left join (
    select data.id,
           data.province_id,
           data.session_id
    from ods_order_info_inc
    where dt="2022-06-09" and type="insert"
)ooii on oodi.order_id=ooii.id
         left join (
    select common.sid ses,
           common.sc sour
    from ods_log_inc
    where dt = '2022-06-09'
)log on ooii.session_id = log.ses
         left join (
    select id,
           source_site
    from ods_base_source_full
    where dt = '2022-06-09'
)sou on log.sour = sou.id;




//3、交易域支付成功事务事实表        (维度字段 日期id ) 度量: 订单id

-------------------------------------------------------------------------------------------------------------------
编号      订单id       用户id         课程id          省份id        下单时间          下单日期
id      order_id     user_id      course_id     province_id    create_time       date_id

价格               优惠金额              实际价格
origin_price      reduce_amount        actual_price;
-------------------------------------------------------------------------------------------------------------------

drop table if exists dwd_trade_trade_pay_detail_suc_inc;
create external table dwd_trade_pay_detail_suc_inc (
                                                       `id`                     STRING COMMENT '编号',
                                                       `order_id`              STRING COMMENT '订单ID',
                                                       `user_id`               STRING COMMENT '用户ID',
                                                       `course_id`                STRING COMMENT '课程ID',
                                                       `province_id`          STRING COMMENT '省份ID',
                                                       session_id              string comment '会话ID',
                                                       `date_id`               STRING COMMENT '支付日期ID',
                                                       `callback_time`           STRING COMMENT '支付时间',
                                                       origin_amount           DECIMAL(16, 2) COMMENT '原始价格',
                                                       coupon_reduce           DECIMAL(16, 2) COMMENT '优惠券减免金额',
                                                       final_amount           DECIMAL(16, 2) COMMENT '最终支付金额'
)COMMENT '交易域支付成功事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dwd/dwd_trade_pay_detail_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');



--首日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select
    oodi.`id`                   ,--  STRING COMMENT '编号',
    opii.`order_id`             ,-- STRING COMMENT '订单ID',
    `user_id`              ,-- STRING COMMENT '用户ID',
    `course_id`            ,--    STRING COMMENT '课程ID',
    `province_id`          ,--STRING COMMENT '省份ID',
    session_id             ,-- string comment '会话ID',
    date_format(callback_time,"yyyy-MM-dd") `date_id`              ,-- STRING COMMENT '下单日期ID',
    callback_time       ,-- STRING COMMENT '下单时间',
    origin_amount          ,-- DECIMAL(16, 2) COMMENT '原始价格',
    coupon_reduce          ,-- DECIMAL(16, 2) COMMENT '优惠券减免金额',
    final_amount           ,--DECIMAL(16, 2) COMMENT '最终金额'
    date_format(callback_time,"yyyy-MM-dd")
from (
         select
             data.`id`                  ,--  STRING COMMENT '编号',
             data.`order_id`            ,-- STRING COMMENT '订单ID',
             data.`user_id`             ,-- STRING COMMENT '用户ID',
             data.`course_id`           ,--    STRING COMMENT '课程ID',
             --data.`province_id`         ,--STRING COMMENT '省份ID',
             --date_format(data.`create_time`,"yyyy-MM-dd")  `date_id`             ,-- STRING COMMENT '下单日期ID',
             --data.`create_time`         ,-- STRING COMMENT '下单时间',
             data.origin_amount         ,-- DECIMAL(16, 2) COMMENT '原始价格',
             data.coupon_reduce         ,-- DECIMAL(16, 2) COMMENT '优惠券减免金额',
             data.final_amount          --DECIMAL(16, 2) COMMENT '最终金额'
         from ods_order_detail_inc
         where dt="2022-06-08" and type="bootstrap-insert"
     )oodi
         join (
    select data.order_id,
           data.callback_time
    from ods_payment_info_inc
    where dt="2022-06-08" and type="bootstrap-insert" and data.payment_status="1602"
)opii on oodi.order_id=opii.order_id
         left join (
    select data.id,
           data.session_id            ,
           data.province_id
    from ods_order_info_inc
    where dt="2022-06-08" and type="bootstrap-insert"
)ooii on oodi.order_id=ooii.id;


--每日
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt="2022-06-09")
select
    oodi.`id`                   ,--  STRING COMMENT '编号',
    opii.`order_id`             ,-- STRING COMMENT '订单ID',
    `user_id`              ,-- STRING COMMENT '用户ID',
    `course_id`            ,--    STRING COMMENT '课程ID',
    `province_id`          ,--STRING COMMENT '省份ID',
    session_id             ,-- string comment '会话ID',
    date_format(callback_time,"yyyy-MM-dd") `date_id`              ,-- STRING COMMENT '下单日期ID',
    callback_time       ,-- STRING COMMENT '下单时间',
    origin_amount          ,-- DECIMAL(16, 2) COMMENT '原始价格',
    coupon_reduce          ,-- DECIMAL(16, 2) COMMENT '优惠券减免金额',
    final_amount           --DECIMAL(16, 2) COMMENT '最终金额'
from (
         select
             data.`id`                  ,--  STRING COMMENT '编号',
             data.`order_id`            ,-- STRING COMMENT '订单ID',
             data.`user_id`             ,-- STRING COMMENT '用户ID',
             data.`course_id`           ,--    STRING COMMENT '课程ID',
             --data.`province_id`         ,--STRING COMMENT '省份ID',
             --date_format(data.`create_time`,"yyyy-MM-dd")  `date_id`             ,-- STRING COMMENT '下单日期ID',
             --data.`create_time`         ,-- STRING COMMENT '下单时间',
             data.origin_amount         ,-- DECIMAL(16, 2) COMMENT '原始价格',
             data.coupon_reduce         ,-- DECIMAL(16, 2) COMMENT '优惠券减免金额',
             data.final_amount          --DECIMAL(16, 2) COMMENT '最终金额'
         from ods_order_detail_inc
         where (dt="2022-06-09" or dt=date_sub("2022-06-09",1)) and
             (type="insert" or type="bootstrap-insert")
     )oodi
         join (
    select data.order_id,
           data.callback_time
    from ods_payment_info_inc
    where dt="2022-06-09" and type="update" and
        array_contains(map_keys(old),"payment_status")and data.payment_status="1602"
)opii on oodi.order_id=opii.order_id
         left join (
    select data.id,
           data.session_id            ,
           data.province_id
    from ods_order_info_inc
    where (dt="2022-06-09" or dt=date_sub("2022-06-09",1)) and
        (type="insert" or type="bootstrap-insert")
)ooii on oodi.order_id=ooii.id;



//4、流量域页面浏览事务事实表        (维度字段 )

-------------------------------------------------------------------------------------------------------------------
   省份id          手机品牌       渠道        是否首次启动      手机型号     设备id        操作系统
province_id        brand      channel       is_new         model      mid_id    operate_system

  会员id       APP版本号       目标ID         目标类型            上页ID           页面id
 user_id    version_code    page_item    page_item_type    last_page_id     page_id

 日期id        跳入时间        所属会话id       停留时间
 date_id     view_time      session_id    during_time
-------------------------------------------------------------------------------------------------------------------

drop table if exists dwd_traffic_page_view_inc;
create external table dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份ID',
    `brand`          STRING COMMENT '手机品牌',
    `source_id`        STRING COMMENT '来源',
    `source_site`        STRING COMMENT '来源名',
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
    `date_id`        STRING COMMENT '日期ID',
    `view_time`      STRING COMMENT '跳入时间',
    `session_id`     STRING COMMENT '所属会话ID',
    `during_time`    BIGINT COMMENT '持续时间毫秒'
) COMMENT '流量域页面浏览事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--数据装载
--set hive.cho.enable=false;
insert overwrite table dwd_traffic_page_view_inc partition (dt = '2022-06-08')
select common.ar                                                           province_id,
       common.ba                                                           brand,
       common.sc                                                           source_id,
       source_site,
       common.is_new                                                       is_new,
       common.md                                                           module,
       common.mid                                                          mid_id,
       common.os                                                           operate_system,
       common.uid                                                          user_id,
       common.vc                                                           version_code,
       page.item                                                           page_item,
       page.item_type                                                      page_item_type,
       page.last_page_id,
       page.page_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') view_time,
       common.sid                                                          session_id,
       page.during_time
from ods_log_inc log join (
    select
        id,
        source_site
    from ods_base_source_full
    where dt='2022-06-08'
) sc on sc.id=log.common.sc
where dt = '2022-06-08'
  and page is not null;
--set hive.cho.enable=true;


//5、用户域登录事务事实表      (维度字段 用户id 日期id 身份id 设备id) 度量:一行登录时间

-------------------------------------------------------------------------------------------------------------------
用户id       日期id     登录时间        渠道          省份id        设备id      设备品牌     设备型号     操作系统
user_id    date_id    login_time    channel    province_id     mid_id      brand       model    operate_system
-------------------------------------------------------------------------------------------------------------------
--用户域 登录
drop table if exists dwd_user_login_inc;
create external table dwd_user_login_inc
(
    user_id        string,
    date_id        string,
    login_time     string,
    channel        string,
    province_id    string,
    mid_id         string,
    brand          string,
    model          string,
    operate_system string
) comment '用户域用户登录事务事实表'
    partitioned by (dt string)
    stored as orc
    location 'warehouse/edu/dwd/dwd_user_login_inc/'
    tblproperties ("orc.compress" = "snappy");



--数据装载
insert overwrite table dwd_user_login_inc partition (dt = '2022-06-08')
select user_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
       channel,
       province_id,
       mid_id,
       brand,
       model,
       operate_system
from (
         select common.uid                                              user_id,
                ts,
                common.ch                                               channel,
                common.ar                                               province_id,
                common.mid                                              mid_id,
                common.ba                                               brand,
                common.md                                               model,
                common.os                                               operate_system,
                row_number() over (partition by common.sid order by ts) rn
         from ods_log_inc
         where dt = '2022-06-08'
           and page is not null
           and common.uid is not null
     ) t1
where rn = 1;


//6、用户注域册事务事实表      (维度字段 ) 第一次登录

-------------------------------------------------------------------------------------------------------------------
用户id       日期id     注册时间        渠道          省份id        设备id      设备品牌     设备型号     操作系统
user_id    date_id   create_time    channel    province_id     mid_id      brand       model    operate_system
-------------------------------------------------------------------------------------------------------------------

--6、用户注域册事务事实表
DROP TABLE IF EXISTS dwd_user_register_inc;
CREATE EXTERNAL TABLE dwd_user_register_inc
(
    `user_id`          STRING COMMENT '用户ID',
    `date_id`          STRING COMMENT '日期ID',
    `create_time`     STRING COMMENT '注册时间',
    `channel`          STRING COMMENT '渠道',
    `province_id`     STRING COMMENT '省份ID',
    `mid_id`           STRING COMMENT '设备ID',
    `brand`            STRING COMMENT '设备品牌',
    `model`            STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dwd/dwd_user_register_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

--每日装载
insert overwrite table dwd_user_register_inc partition (dt = '2022-06-08')
select  ui.`user_id`    ,--      STRING COMMENT '用户ID',
        date_format(create_time,'yyyy-MM-dd')  `date_id`      ,--    STRING COMMENT '日期ID',
        `create_time`   ,--  STRING COMMENT '注册时间',
        `channel`        ,--  STRING COMMENT '渠道',
        `province_id`    ,-- STRING COMMENT '省份ID',
        `mid_id`         ,--  STRING COMMENT '设备ID',
        `brand`          ,--  STRING COMMENT '设备品牌',
        `model`           ,-- STRING COMMENT '设备型号',
        `operate_system` --STRING COMMENT '设备操作系统'
from(
        select data.id user_id,
               data.create_time
        from ods_user_info_inc
        where dt = '2022-06-08'
          and type = 'insert'
    ) ui
        left join
    (
        select province_id,
               brand,
               channel,
               model,
               mid_id,
               user_id,
               operate_system
        from(select common.ar province_id,
                    common.ba brand,
                    common.ch channel,
                    common.md model,
                    common.mid mid_id,
                    common.uid user_id,
                    common.os operate_system,
                    row_number() over (partition by common.uid order by ts) rw
             from ods_log_inc
             where dt = '2022-06-08'
               and common.uid is not null)t
        where rw = 1
    )log
    on ui.user_id = log.user_id;


--每日装载
--每日装载
insert overwrite table dwd_user_register_inc partition (dt = '2022-06-09')
select  ui.`user_id`    ,--      STRING COMMENT '用户ID',
        date_format(create_time,'yyyy-MM-dd')  `date_id`      ,--    STRING COMMENT '日期ID',
        `create_time`   ,--  STRING COMMENT '注册时间',
        `channel`        ,--  STRING COMMENT '渠道',
        `province_id`    ,-- STRING COMMENT '省份ID',
        `mid_id`         ,--  STRING COMMENT '设备ID',
        `brand`          ,--  STRING COMMENT '设备品牌',
        `model`           ,-- STRING COMMENT '设备型号',
        `operate_system` --STRING COMMENT '设备操作系统'
from(
        select data.id user_id,
               data.create_time
        from ods_user_info_inc
        where dt = '2022-06-09'
          and type = 'insert'
    ) ui
        left join
    (
        select province_id,
               brand,
               channel,
               model,
               mid_id,
               user_id,
               operate_system
        from(select common.ar province_id,
                    common.ba brand,
                    common.ch channel,
                    common.md model,
                    common.mid mid_id,
                    common.uid user_id,
                    common.os operate_system,
                    row_number() over (partition by common.uid order by ts) rw
             from ods_log_inc
             where dt = '2022-06-09'
               and common.uid is not null)t
        where rw = 1
    )log
    on ui.user_id = log.user_id;


//7、互动域收藏商品事务事实表      (维度字段 课程id 用户id) 度量：一次收藏

-------------------------------------------------------------------------------------------------------------------
编号      用户id      课程id      日期id         收藏时间
id      user_id    course_id   date_id      create_time
-------------------------------------------------------------------------------------------------------------------

------------------------------互动域收藏商品事务事实表--------------------------------
DROP TABLE IF EXISTS dwd_interaction_favor_add_inc;
CREATE EXTERNAL TABLE dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `course_id`      STRING COMMENT '课程id',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '互动域收藏商品事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dwd/dwd_interaction_favor_add_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");


--首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_interaction_favor_add_inc partition (dt)
select  data.`id`     ,--     STRING COMMENT '编号',
        data.`user_id`    ,-- STRING COMMENT '用户ID',
        data.`course_id`  ,--    STRING COMMENT '课程id',
        date_format(data.create_time,'yyyy-MM-dd') `date_id`    ,-- STRING COMMENT '日期ID',
        data.`create_time`,-- STRING COMMENT '收藏时间'
        date_format(data.create_time,'yyyy-MM-dd')
from ods_favor_info_inc
where dt = '2022-06-08'
  and type = 'bootstrap-insert';

--每日装载
insert overwrite table dwd_interaction_favor_add_inc partition (dt = '2022-06-09')
select  data.`id`     ,--     STRING COMMENT '编号',
        data.`user_id`    ,-- STRING COMMENT '用户ID',
        data.`course_id`  ,--    STRING COMMENT '课程id',
        date_format(data.create_time,'yyyy-MM-dd') `date_id`    ,-- STRING COMMENT '日期ID',
        data.`create_time`-- STRING COMMENT '收藏时间'
from ods_favor_info_inc
where dt = '2022-06-08'
  and type = 'insert'



//8、互动域课程评价事务事实表       (维度字段 user id 课程id) 度量 : 星数

-------------------------------------------------------------------------------------------------------------------
编号      用户id     课程id       评分           日期id     评价时间
id      user_id   course_id  review_stars    date_id    create_time
-------------------------------------------------------------------------------------------------------------------
---------------------------------互动域课程评价事务事实表----------------------------------------
DROP TABLE IF EXISTS dwd_interaction_course_review_inc;
CREATE EXTERNAL TABLE dwd_interaction_course_review_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `course_id`      STRING COMMENT '课程id',
    `review_stars`      BIGINT COMMENT '评分',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '互动域收藏商品事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dwd/dwd_interaction_course_review_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

--首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_interaction_course_review_inc partition (dt)
select  data.`id`          ,--STRING COMMENT '编号',
        data.`user_id`     ,--STRING COMMENT '用户ID',
        data.`course_id`   ,--   STRING COMMENT '课程id',
        data.`review_stars`,--      BIGINT COMMENT '评分',
        date_format(data.create_time,'yyyy-MM-dd') `date_id`     ,--STRING COMMENT '日期ID',
        data.`create_time` ,--STRING COMMENT '收藏时间'
        date_format(data.create_time,'yyyy-MM-dd')
from ods_review_info_inc
where dt = '2022-06-08'
  and type = 'bootstrap-insert';

--每日装载
insert overwrite table dwd_interaction_course_review_inc partition (dt = '2022-06-09')
select  data.`id`          ,--STRING COMMENT '编号',
        data.`user_id`     ,--STRING COMMENT '用户ID',
        data.`course_id`   ,--   STRING COMMENT '课程id',
        data.`review_stars`,--      BIGINT COMMENT '评分',
        date_format(data.create_time,'yyyy-MM-dd') `date_id`     ,--STRING COMMENT '日期ID',
        data.`create_time` --STRING COMMENT '收藏时间'
from ods_review_info_inc
where dt = '2022-06-08'
  and type = 'insert';


//9、考试域答题事务事实表     (维度字段 试卷id 用户id )

-------------------------------------------------------------------------------------------------------------------
用户id       问题id        考试id       试卷id       是否正确       得分      答题时间        答题日期
user_id  question_id    exam_id      paper_id    is_correct    score    create_time    date_id
-------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS dwd_exam_paper_inc;
create external table dwd_exam_paper_inc
(
    `id`           STRING COMMENT '编号',
    `user_id`      STRING COMMENT '用户ID',
    `paper_id`     STRING COMMENT '试卷id',
    score          decimal(16, 2) COMMENT '分数',
    `duration_sec` bigint COMMENT '时长',
    `create_time`  string COMMENT '答卷时间',
    `submit_time`  string COMMENT '交卷时间',
    `data_id`      string COMMENT '日期id',
    course_id      string comment '课程id',
    paper_title    string comment '试卷名称'
) COMMENT '考试域答卷事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dwd/dwd_exam_paper_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

//首日
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_exam_paper_inc partition (dt)
select t1.id,
       user_id,
       paper_id,
       score,
       duration_sec,
       create_time,
       submit_time,
       data_id,
       course_id,
       paper_title,
       dt
from (
         select data.id,
                data.user_id,
                data.paper_id,
                data.score,
                data.duration_sec,
                data.create_time,
                data.submit_time,
                date_format(data.create_time, 'yyyy-MM-dd') data_id,
                date_format(data.create_time, 'yyyy-MM-dd') dt
         from ods_test_exam_inc
         where type = 'bootstrap-insert'
     ) t1
         left join
     (select id,
             course_id,
             paper_title
      from ods_test_paper_full
      where dt = '2022-06-08') t2 on t1.paper_id = t2.id;





//10、考试域答题事务事实表     (维度字段 考试id 试卷id 用户id 问题id)

-------------------------------------------------------------------------------------------------------------------
编号     用户id       试卷id        得分        所用时间         提交时间       答卷时间        答卷日期
id      user_id     paper_id     score    duration_time    submit_time   create_time    date_id
-------------------------------------------------------------------------------------------------------------------
--11、考试域答题事务事实表
DROP TABLE IF EXISTS dwd_exam_question_inc;

create external table dwd_exam_question_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `question_id`      STRING COMMENT '题目id',
    `exam_id`     STRING COMMENT '测试ID',
    `paper_id` STRING COMMENT '试卷id',
    `date_id` STRING COMMENT '日期id',
    `is_correct`     BIGINT COMMENT '是否正确',
    score bigint COMMENT '分数',
    create_time STRING COMMENT '答题时间'
) COMMENT '考试域答题事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dwd/dwd_exam_question_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

//首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_exam_question_inc partition (dt);
select
    t1.id,
    t1.user_id,
    t1.question_id,
    t1.exam_id,
    t1.paper_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    is_correct,
    score,
    create_time,
    date_format(create_time,'yyyy-MM-dd')
from
    (
        select
            data.id,
            data.exam_id,
            data.paper_id,
            data.question_id,
            data.user_id,
            data.is_correct,
            data.score,
            data.create_time
        from ods_test_exam_question_inc
        where type='bootstrap-insert'
    )t1;

select * from ods_test_exam_question_inc
where date_format(data.create_time,'yyyy-MM-hh')='2022-06-09';

//每日装载

insert overwrite table dwd_exam_question_inc partition (dt='2022-06-09')
select
    t1.id,
    t1.user_id,
    t1.question_id,
    t1.exam_id,
    t1.paper_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    is_correct,
    score,
    create_time
from
    (
        select
            data.id,
            data.exam_id,
            data.paper_id,
            data.question_id,
            data.user_id,
            data.is_correct,
            data.score,
            data.create_time
        from ods_test_exam_question_inc
        where type='insert' and dt='2022-06-09'
    )t1;



//11、视频域播放视频事务事实表（增量）       (维度字段 课程id，章节id，用户id)

-------------------------------------------------------------------------------------------------------------------
用户id       视频id       章节id         课程id        播放时长         播放进度
user_id    video_id    chapter_id    course_id     play_sec      position_sec

   渠道          省份id        设备id      设备品牌     设备型号     操作系统         会话id
 channel     province_id     mid_id      brand       model    operate_system    session_id
-------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS dwd_video_play_inc;
create external table dwd_video_play_inc
(
    `user_id`     STRING COMMENT '用户ID',
    chapter_id      string,
    `course_id`      STRING COMMENT '课程id',
    video_id string,
    video_name string,
    `session_id` string COMMENT '会话id',
    `play_sec`      STRING COMMENT '播放时长',
    `position_sec`      STRING COMMENT '播放进度',
    `channel`      STRING COMMENT '播放渠道',
    `province_id`      STRING COMMENT '身份id',
    `mid_id`      STRING COMMENT '硬件id',
    `brand`      STRING COMMENT '品牌',
    `model`     string COMMENT '型号',
    `operate_system`     string COMMENT 'os',
    `create_time`     string COMMENT '创建时间'
) COMMENT '视频域播放视频事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dwd/dwd_video_play_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//首日
insert overwrite table dwd_video_play_inc partition (dt)
select
    uid,
    chapter_id,
    course_id,
    video_id,
    video_name,
    sid,
    play_sec,
    position_sec,
    ch,
    ar,
    mid,
    ba,
    md,
    os,
    create_time,
    date_format(create_time,'yyyy-MM-dd')
from
(
    select
        common.ar,
        common.    ba,
        common.ch,
        common.is_new,
        common.md,
        common.mid,
        common.os,
        common.sc,
        common.sid,
        common.uid,
        common.vc,
        appvideo.play_sec,
        appvideo.position_sec,
        appvideo.video_id,
        date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') create_time
    from ods_log_inc
    where appvideo is not null
    and dt='2022-06-08'
) t1 join (
    select
        id,
        video_name,
        chapter_id,
        course_id
    from ods_video_info_full
    where dt='2022-06-08'
) t2 on t1.video_id=t2.id;

//每日
insert overwrite table dwd_video_play_inc partition (dt='2022-06-08')
select
    uid,
    chapter_id,
    course_id,
    video_id,
    video_name,
    sid,
    play_sec,
    position_sec,
    ch,
    ar,
    mid,
    ba,
    md,
    os,
    create_time
from
    (
        select
            common.ar,
            common.    ba,
            common.ch,
            common.is_new,
            common.md,
            common.mid,
            common.os,
            common.sc,
            common.sid,
            common.uid,
            common.vc,
            appvideo.play_sec,
            appvideo.position_sec,
            appvideo.video_id,
            date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') create_time
        from ods_log_inc
        where appvideo.video_id is not null
        and dt='2022-06-08'
    ) t1 join (
        select
            id,
            video_name,
            chapter_id,
            course_id
        from ods_video_info_full
        where dt='2022-06-08'
    ) t2 on t1.video_id=t2.id;

//12、视频域播放视频事务事实表（全量）
DROP TABLE IF EXISTS dwd_video_play_full;
create external table dwd_video_play_full
(
    `user_id`     STRING COMMENT '用户ID',
    chapter_id      string,
    `course_id`      STRING COMMENT '课程id',
    video_id string,
    `play_sec`      bigint COMMENT '播放时长',
    `position_sec`      bigint COMMENT '播放进度',
    `create_time`     string COMMENT '创建时间'
) COMMENT '视频域播放视频周期快照事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dwd/dwd_video_play_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


//首日
insert overwrite table dwd_video_play_full partition (dt='2022-06-08')
select
    uid,
    chapter_id,
    course_id,
    video_id,
    play_sec,
    position_sec,
    create_time
from
       ( select
            common.uid,
            sum(appvideo.play_sec) play_sec,
            max(cast(appvideo.position_sec as bigint)) position_sec,
            appvideo.video_id,
            date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd') create_time
        from ods_log_inc
        where appvideo is not null
          and dt='2022-06-08'
        group by common.uid,appvideo.video_id,date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')
    ) t1 join (
        select
            id,
            chapter_id,
            course_id
        from ods_video_info_full
        where dt='2022-06-08'
    ) t2 on t1.video_id=t2.id;


//每日
insert overwrite table dwd_video_play_full partition (dt='2022-06-09');
select
    user_id,
    old.chapter_id,
    old.course_id,
    old.video_id,
    nvl(t1.play_sec,0)+nvl(old.play_sec,0),
   `if`(t1.uid is null ,old.position_sec,t1.play_sec),
    t1.create_time
from
    ( select
          common.uid,
          sum(appvideo.play_sec) play_sec,
          max(cast(appvideo.position_sec as bigint)) position_sec,
          appvideo.video_id,
          date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd') create_time
      from ods_log_inc
      where appvideo is not null
        and dt='2022-06-09'
      group by common.uid,appvideo.video_id,date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')
    ) t1 join (
        select
            id,
            chapter_id,
            course_id
        from ods_video_info_full
        where dt='2022-06-08'
    ) t2 on t1.video_id=t2.id
         full join (
        select      user_id,
                    chapter_id,
                    course_id,
                    video_id,
                    play_sec,
                    position_sec,
                    create_time
        from dwd_video_play_full
        where dt=date_sub('2022-06-09',1)
    ) old on t1.video_id=old.video_id and t1.uid=old.user_id;



