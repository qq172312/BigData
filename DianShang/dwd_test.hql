

insert overwrite table dwd_trade_trade_flow_acc partition (dt)
select
    t1.user_id,
    t1.order_id,
    province_id,
    order_date_id,
    order_time,
    nvl(payment_date_id,date_format(t2.callback_time,'yyyy-MM-hh')),
    nvl(payment_time,date_format(t2.callback_time)),
    nvl(date_format(t3.create_time,'yyyy-MM-hh'),t1.finish_date_id),
    nvl(t3.create_time,t1.finish_time),
    order_original_amount,
    order_activity_amount,
    order_coupon_amount,
    order_total_amount,
    t2.total_amount,
    nvl(date_format(t3.create_time,'yyyy-MM-hh'),'9999-12-31')
from (select user_id,
             order_id,
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
      union
      select data.user_id,
             data.id order_id,
             data.province_id,
             date_format(data.create_time, 'yyyy-MM-hh') order_date_id,
             data.create_time  order_time,
             null payment_date_id,
             null payment_time,
             null finish_date_id,
             null finish_time,
             data.original_total_amount  order_original_amount,
             data.activity_reduce_amount    order_activity_amount,
             data.coupon_reduce_amount  order_coupon_amount,
             data.total_amount  order_total_amount,
             null payment_amount
      from ods_order_info_inc
      where dt = '2022-06-08'
        and type = 'insert' ) t1
         left join (select
                        data.order_id,
                        data.total_amount,
                        data.payment_status,
                        data.callback_time
                    from ods_payment_info_inc
                    where
                    dt='2022-06-08'
                       and type = 'update'
                      and array_contains(map_values(old), 'payment-status')
                    and data.payment_status='1602'
                    ) t2 on t1.order_id=t2.order_id
left join
    (select
         data.order_id,
         data.order_status,
         data.create_time
     from ods_order_status_log_inc
     where dt='2022-06-08' and type='bootstrap-insert'
     and data.order_status='1004'
     )t3 on t3.order_id=t1.order_id;



select *
from ods_order_status_log_inc
where data.order_id='38040';

