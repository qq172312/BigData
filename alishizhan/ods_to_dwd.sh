#!/bin/bash

APP=edu
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dwd_user_register_inc="
insert overwrite table ${APP}.dwd_user_register_inc partition (dt = '$do_date')
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
        from ${APP}.ods_user_info_inc
        where dt = '$do_date'
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
             from ${APP}.ods_log_inc
             where dt = '$do_date'
               and common.uid is not null)t
        where rw = 1
    )log
    on ui.user_id = log.user_id;
    "

dwd_interaction_favor_add_inc="
insert overwrite table ${APP}.dwd_interaction_favor_add_inc partition (dt = '$do_date')
select  data.`id`     ,--     STRING COMMENT '编号',
        data.`user_id`    ,-- STRING COMMENT '用户ID',
        data.`course_id`  ,--    STRING COMMENT '课程id',
        date_format(data.create_time,'yyyy-MM-dd') `date_id`    ,-- STRING COMMENT '日期ID',
        data.`create_time`-- STRING COMMENT '收藏时间'
from ${APP}.ods_favor_info_inc
where dt = '$do_date'
  and type = 'insert';
"

dwd_interaction_course_review_inc="
insert overwrite table ${APP}.dwd_interaction_course_review_inc partition (dt = '$do_date')
select  data.`id`          ,--STRING COMMENT '编号',
        data.`user_id`     ,--STRING COMMENT '用户ID',
        data.`course_id`   ,--   STRING COMMENT '课程id',
        data.`review_stars`,--      BIGINT COMMENT '评分',
        date_format(data.create_time,'yyyy-MM-dd') `date_id`     ,--STRING COMMENT '日期ID',
        data.`create_time` --STRING COMMENT '收藏时间'
from ${APP}.ods_review_info_inc
where dt = '$do_date'
  and type = 'insert';
"

dwd_exam_question_inc="
insert overwrite table ${APP}.dwd_exam_question_inc partition (dt='$do_date')
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
            data. id,
            data.     exam_id,
            data. paper_id,
            data. question_id,
            data. user_id,
            data. is_correct,
            data. score,
            data. create_time
        from ${APP}.ods_test_exam_question_inc
        where type='insert' and dt='$do_date'
    )t1;
"

dwd_exam_paper_inc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_exam_paper_inc partition (dt='$do_date')
select
    *
from (
         select
             data. id,
             data. user_id,
             data. paper_id,
             data. score,
             data. duration_sec,
             data. create_time,
             data. submit_time,
             date_format(data.create_time,'yyyy-MM-dd')
         from ${APP}.ods_test_exam_inc
         where type='insert' and dt='$do_date'
     ) t1;"

dwd_video_play_inc="
insert overwrite table ${APP}.dwd_video_play_inc partition (dt='$do_date')
select
    uid,
    chapter_id,
    course_id,
    video_id,
    video_name,
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
        from ${APP}.ods_log_inc
        where appvideo is not null
        and dt='$do_date'
    ) t1 join (
        select
            id,
            video_name,
            chapter_id,
            course_id
        from ${APP}.ods_video_info_full
        where dt='$do_date'
    ) t2 on t1.video_id=t2.id;
"

dwd_trade_cart_add_inc="
insert overwrite table ${APP}.dwd_trade_cart_add_inc partition (dt="$do_date")
select data.`id`                 ,-- STRING COMMENT '编号',
       data.`user_id`            ,--STRING COMMENT '用户ID',
       data.`course_id`          ,--   STRING COMMENT '课程ID',
       data.session_id           ,
       date_format(from_utc_timestamp(ts*1000,"GMT+8"),"yyyy-MM-dd") date_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss')  create_time,
       if(type="insert",data.cart_price,cast(data.cart_price as DECIMAL(16,2))-cast(old["cart_price"] as DECIMAL(16,2))) cart_price
from ${APP}.ods_cart_info_inc
where dt="$do_date" and (type="insert" or (type="update" and old["cart_price"] is not null and
                                             cast(data.cart_price as DECIMAL(16,2))>cast(old["cart_price"] as DECIMAL(16,2))));                                       cast(data.cart_price as DECIMAL(16,2))>cast(old["cart_price"] as DECIMAL(16,2))));
"

dwd_trade_order_detail_inc="
insert overwrite table ${APP}.dwd_trade_order_detail_inc partition (dt='$do_date')
select
    oodi.`id`                   ,--  STRING COMMENT '编号',
    `order_id`             ,-- STRING COMMENT '订单ID',
    `user_id`              ,-- STRING COMMENT '用户ID',
    `course_id`            ,--    STRING COMMENT '课程ID',
    `province_id`          ,--STRING COMMENT '省份ID',
    session_id             ,-- string comment '会话ID',
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
             --data.`province_id`         ,--STRING COMMENT '省份ID',
             date_format(data.`create_time`,'yyyy-MM-dd')  `date_id`             ,-- STRING COMMENT '下单日期ID',
             data.`create_time`         ,-- STRING COMMENT '下单时间',
             data.origin_amount         ,-- DECIMAL(16, 2) COMMENT '原始价格',
             data.coupon_reduce         ,-- DECIMAL(16, 2) COMMENT '优惠券减免金额',
             data.final_amount          --DECIMAL(16, 2) COMMENT '最终金额'
         from ${APP}.ods_order_detail_inc
         where dt='$do_date' and type='insert'
     )oodi
         left join (
    select data.id,
           data.province_id,
           data.session_id
    from ${APP}.ods_order_info_inc
    where dt='$do_date' and type='insert'
)ooii on oodi.order_id=ooii.id;
"

dwd_trade_pay_detail_suc_inc="
insert overwrite table ${APP}.dwd_trade_pay_detail_suc_inc partition (dt="$do_date")
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
         where (dt="$do_date" or dt=date_sub("$do_date",1)) and
             (type="insert" or type="bootstrap-insert")
     )oodi
         join (
    select data.order_id,
           data.callback_time
    from ${APP}.ods_payment_info_inc
    where dt="$do_date" and type="update" and
        array_contains(map_keys(old),"payment_status")and data.payment_status="1602"
)opii on oodi.order_id=opii.order_id
         left join (
    select data.id,
           data.session_id            ,
           data.province_id
    from ${APP}.ods_order_info_inc
    where (dt="$do_date" or dt=date_sub("$do_date",1)) and
        (type="insert" or type="bootstrap-insert")
)ooii on oodi.order_id=ooii.id;
"

dwd_traffic_page_view_inc="
insert overwrite table ${APP}.dwd_traffic_page_view_inc partition (dt = '$do_date')
select common.ar                                                           province_id,
       common.ba                                                           brand,
       common.ch                                                           brand,
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
from ${APP}.ods_log_inc
where dt = '$do_date'
  and page is not null;
"

dwd_user_login_inc="
insert overwrite table ${APP}.dwd_user_login_inc partition (dt = '$do_date')
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
         from ${APP}.ods_log_inc
         where dt = '$do_date'
           and page is not null
           and common.uid is not null
     ) t1
where rn = 1
"

case $1 in
    "dwd_user_register_inc" )
        hive -e "$dwd_user_register_inc"
    ;;
    "dwd_interaction_favor_add_inc" )
        hive -e "$dwd_interaction_favor_add_inc"
    ;;
    "dwd_interaction_course_review_inc" )
        hive -e "$dwd_interaction_course_review_inc"
    ;;
    "dwd_exam_question_inc" )
        hive -e "$dwd_exam_question_inc"
    ;;   
    "dwd_exam_paper_inc" )
        hive -e "$dwd_exam_paper_inc"
    ;;  
    "dwd_video_play_inc" )
        hive -e "$dwd_video_play_inc"
    ;;
    "dwd_trade_cart_add_inc" )
        hive -e "$dwd_interaction_favor_add_inc"
    ;;
    "dwd_trade_order_detail_inc" )
        hive -e "$dwd_trade_order_detail_inc"
    ;;
    "dwd_trade_pay_detail_suc_inc" )
        hive -e "$dwd_user_register_inc"
    ;;   
    "dwd_traffic_page_view_inc" )
        hive -e "$dwd_traffic_page_view_inc"
    ;;
    "dwd_user_login_inc" )
          hive -e "$dwd_user_login_inc"
    ;;
    "all" )
        hive -e "$dwd_user_register_inc$dwd_interaction_favor_add_inc$dwd_interaction_course_review_inc$dwd_exam_question_inc$dwd_exam_paper_inc$dwd_video_play_inc$dwd_trade_cart_add_inc$dwd_trade_order_detail_inc$dwd_trade_pay_detail_suc_inc$dwd_traffic_page_view_inc$dwd_user_login_inc"
    ;;
esac
