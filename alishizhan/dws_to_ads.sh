#!/bin/bash

APP=edu
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

ads_traffic_stats_by_source="
insert overwrite table ${APP}.ads_traffic_stats_by_source
select * from ${APP}.ads_traffic_stats_by_source
union
select '$do_date',
       recent_days,
       source_site,
       cast(count(distinct mid_id) as bigint) ,
       cast(avg(during_time_1d)/1000 as bigint),
       cast(avg(page_count_1d) as bigint),
       cast(count(*) as bigint),
       cast(sum(if(page_count_1d = 1,1,0)) / count(*) as decimal(16,2))
from ${APP}.dws_traffic_session_page_view_1d lateral view explode (array(1,7,30)) tmp as recent_days
where dt >= date_sub('$do_date',recent_days - 1)
group by recent_days,source_site;
"
ads_page_path="
insert overwrite table ${APP}.ads_page_path
select * from ${APP}.ads_page_path
union
select '$do_date' dt,
       source,
       nvl(target,'null'),
       count(*) path_count
from (select concat('step-',rn,':',page_id) source,
             concat('step-',rn+1,':',next_page_id) target
      from (select page_id,
                   lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
                   row_number() over (partition by session_id order by view_time) rn
            from dwd_traffic_page_view_inc
            where dt = '$do_date') t1)t2
group by source,target;
"
ads_order_amount_by_source="
insert overwrite table ${APP}.ads_order_amount_by_source
select *
from ${APP}.ads_order_amount_by_source
union
select '$do_date',
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
      where dt >= date_sub('$do_date',days - 1) and dt <='$do_date'
      group by days,source_id,source_name) t
         left join (
    select rec,
           source_id,
           count(distinct mid_id) sour_num
    from ${APP}.dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as rec
    where dt >= date_sub('$do_date',rec - 1) and dt <= '$do_date'
    group by source_id,rec) t1
                   on t.source_id = t1.source_id and t.days = t1.rec
where t.source_id is not null;
"

ads_user_change="
insert overwrite table ${APP}.ads_user_change
select * from ${APP}.ads_user_change
union
select t3.dt,
       user_churn_count,
       user_back_count
from (
         select '$do_date' dt ,
                count(*) user_churn_count
         from ${APP}.dws_user_user_login_td
         where dt='$do_date' and login_date_last = date_sub('$do_date',7)
     )t3
         join (
    select '$do_date' dt,
           count(*) user_back_count
    from (
             select user_id,
                    '$do_date' dt,
                    login_date_last
             from ${APP}.dws_user_user_login_td
             where dt = '$do_date' and login_date_last='$do_date'
         )t1
             left join (
        select user_id,
               login_date_last
        from ${APP}.dws_user_user_login_td
        where dt = date_sub('$do_date',1)
    )t2 on t1.user_id=t2.user_id
    where datediff(t1.login_date_last,t2.login_date_last) >=8
)t4 on t3.dt=t4.dt;
"

ads_user_retention="
insert overwrite table ${APP}.ads_user_retention
select * from ${APP}.ads_user_retention
union
select '$do_date' dt,
       login_date_first create_date,
       datediff('$do_date', login_date_first) retention_day,
       sum(if(login_date_last = '$do_date', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '$do_date', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                login_date_last,
                login_date_first
         from ${APP}.dws_user_user_login_td
         where dt = '$do_date'
           and login_date_first >= date_add('$do_date', -7)
           and login_date_first < '$do_date'
     ) t1
group by login_date_first;
"

ads_user_stats="
insert overwrite table ${APP}.ads_user_stats
select * from ${APP}.ads_user_stats
union
select '$do_date' dt ,
       recent_days,
       sum(`if`(login_date_first>=date_sub('$do_date',recent_days-1),1,0)) new_user_count,
       sum(`if`(login_date_last>=date_sub('$do_date',recent_days-1),1,0)) active_user_count
from ${APP}.dws_user_user_login_td lateral view explode(`array`(1,7,30)) tmp as recent_days
where dt = '$do_date'
group by dt,recent_days;
"

ads_user_action="
insert overwrite table ${APP}.ads_user_action
select *
from ${APP}.ads_user_action
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
         from ${APP}.dws_traffic_page_visitor_page_view_1d
         where dt='$do_date'
         group by dt
     )t1
         left join (
    select dt,
           count(*) cart_count
    from ${APP}.dws_trade_user_cart_add_1d
    where dt='$do_date'
    group by dt
)t2 on t1.dt=t2.dt
         left join (
    select dt,
           count(*) order_count
    from ${APP}.dws_trade_user_order_1d
    where dt='$do_date'
    group by dt
)t3 on t2.dt=t3.dt
         left join (
    select dt,
           count(*) payment_count
    from ${APP}.dws_trade_user_payment_1d
    where dt='$do_date'
    group by dt
)t4 on t3.dt=t4.dt;
"

ads_order_stats_by_category="
insert overwrite table ${APP}.ads_order_stats_by_category
select * from ${APP}.ads_order_stats_by_category
union
select
    '$do_date' dt,
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
from ${APP}.dws_trade_user_sku_order_1d
where dt='$do_date'
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
from ${APP}.dws_trade_user_sku_order_nd
lateral view explode(array(7,30)) tmp as recent_days
where dt='$do_date') t1
group by recent_days,category_id,category_name) t2;
"
ads_order_stats_by_subject="
insert overwrite table ${APP}.ads_order_stats_by_subject
select * from ${APP}.ads_order_stats_by_subject
union
select
    '$do_date' dt,
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
     from ${APP}.dws_trade_user_sku_order_1d
     where dt='$do_date'
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
          from ${APP}.dws_trade_user_sku_order_nd
                   lateral view explode(array(7,30)) tmp as recent_days
          where dt='$do_date') t1
     group by recent_days,subject_id,subject_name) t2;
"
ads_order_stats_by_course="
insert overwrite table ${APP}.ads_order_stats_by_course
select * from ${APP}.ads_order_stats_by_course
union
select
    '$do_date' dt,
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
     from ${APP}.dws_trade_user_sku_order_1d
     where dt='$do_date'
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
          from ${APP}.dws_trade_user_sku_order_nd
                   lateral view explode(array(7,30)) tmp as recent_days
          where dt='$do_date') t1
     group by recent_days,course_id,course_name) t2;
     "

ads_review_stats_by_course="
insert overwrite table ${APP}.ads_review_stats_by_course
select *
from ${APP}.ads_review_stats_by_course
union
select
    '$do_date' dt,
    recent_days,
    course_id,
    course_name,
    cast(sum(sum_star)/sum(use_count) as bigint),
    sum(use_count),
    cast(sum(good_count)/sum(use_count) as decimal(16,2))
from ${APP}.dws_interaction_course_review_1d
lateral view explode(array(1,7,30)) tmp as recent_days
where dt>date_sub('$do_date',recent_days)
group by recent_days,course_id,course_name;
"
ads_first_play_stats_by_category="
insert overwrite table ${APP}.ads_first_play_stats_by_category
select *
from ${APP}.ads_first_play_stats_by_category
union
select
    '$do_date' dt,
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
     where dt>date_sub('$do_date',recent_days)
     group by recent_days,course_id,user_id) t1
        left join (
        select
            recent_days,
            course_id,
            user_id
        from dwd_trade_order_detail_inc
                 lateral view explode(array(1,2,3,4,5,6,7)) tmp as recent_days
        where dt>date_sub('$do_date',recent_days)
    ) t2 on t1.user_id=t2.user_id and t1.course_id=t2.course_id and t1.recent_days=t2.recent_days
         join ${APP}.dim_course_full t3
             on t1.course_id=t3.id
group by t1.recent_days,t3.category_id,t3.category_name;
"
ads_first_play_stats_by_subject="
insert overwrite table ${APP}.ads_first_play_stats_by_subject
select *
from ${APP}.ads_first_play_stats_by_subject
union
select
    '$do_date' dt,
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
     where dt>date_sub('$do_date',recent_days)
     group by recent_days,course_id,user_id) t1
        left join (
        select
            recent_days,
            course_id,
            user_id
        from dwd_trade_order_detail_inc
                 lateral view explode(array(1,2,3,4,5,6,7)) tmp as recent_days
        where dt>date_sub('$do_date',recent_days)
    ) t2 on t1.user_id=t2.user_id and t1.course_id=t2.course_id and t1.recent_days=t2.recent_days
        join ${APP}.dim_course_full t3
             on t1.course_id=t3.id
group by t1.recent_days,t3.subject_id,t3.subject_name;
"

ads_first_play_stats_by_course="
insert overwrite table ${APP}.ads_first_play_stats_by_course
select *
from ${APP}.ads_first_play_stats_by_course
union
select
    '$do_date' dt,
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
where dt>date_sub('$do_date',recent_days)
group by recent_days,course_id,user_id) t1
left join (
    select
        recent_days,
        course_id,
        user_id
    from dwd_trade_order_detail_inc
             lateral view explode(array(1,2,3,4,5,6,7)) tmp as recent_days
    where dt>date_sub('$do_date',recent_days)
) t2 on t1.user_id=t2.user_id and t1.course_id=t2.course_id and t1.recent_days=t2.recent_days
join ${APP}.dim_course_full t3
on t1.course_id=t3.id
group by t1.recent_days,t1.course_id,t3.course_name;
"

ads_order_all="
insert overwrite table ${APP}.ads_order_all
select *
from ${APP}.ads_order_all
union
select '$do_date' dt,
       1            recent_days,
       sum(order_total_amount_1d),
       sum(order_count_1d),
       count(user_id)
from ${APP}.dws_trade_user_order_1d
where dt = '$do_date'
union
select '$do_date' dt,
       7            recent_days,
       sum(order_total_amount_1d),
       sum(order_count_1d),
       count(distinct user_id)
from ${APP}.dws_trade_user_order_1d
where dt >= date_add('$do_date', -6)
  and dt <= '$do_date'
union
select '$do_date' dt,
       30           recent_days,
       sum(order_total_amount_1d),
       sum(order_count_1d),
       count(distinct user_id)
from ${APP}.dws_trade_user_order_1d
where dt >= date_add('$do_date', -29)
  and dt <= '$do_date';
"

ads_order_by_province="
insert overwrite table ${APP}.ads_order_by_province
select *
from ${APP}.ads_order_by_province
union
select '$do_date' dt,
       1            recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       sum(order_total_amount_1d),
       sum(order_count_1d),
       count(distinct user_id)
from ${APP}.dws_trade_user_sku_order_1d
where dt = '$do_date'
group by province_id,
         province_name,
         area_code,
         iso_code,
         iso_3166_2
union
select '$do_date' dt,
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
from ${APP}.dws_trade_province_order_nd lateral view explode(array(7, 30)) tmp as recent_days
where dt = '$do_date';
"

ads_exam_stats_by_paper="
insert overwrite table ${APP}.ads_exam_stats_by_paper
select * from ${APP}.ads_exam_stats_by_paper
union
select '$do_date',
       1,
       paper_id,
       paper_title,
       score_avg,
       duration_sec_avg,
       paper_num
from ${APP}.dws_exam_course_paper_1d
where dt = '$do_date'
union
select '$do_date',
       recent_days,
       paper_id,
       paper_title,
       case recent_days when 7 then score_avg_7d
                        when 30 then score_avg_30d end ,
       case recent_days when 7 then duration_sec_avg_7d
                        when 30 then duration_sec_avg_30d end,
       case recent_days when 7 then paper_num_7d
                        when 30 then paper_num_30d end
from ${APP}.dws_exam_course_paper_nd lateral view explode(array(7,30)) tmp as recent_days
where dt = '$do_date';
"

ads_exam_course_stats_by_paper="
insert overwrite table ${APP}.ads_exam_course_stats_by_paper
select *
from ${APP}.ads_exam_course_stats_by_paper
union
(select '$do_date',
        t.recent_days,
        t.course_id,
        t1.course_name,
        avg_score,
        avg_during_time,
        user_num
 from (select '$do_date',
              recent_days,
              course_id,
              cast(avg(score) as bigint)        avg_score,
              cast(avg(duration_sec) as bigint) avg_during_time,
              count(distinct user_id)           user_num
       from dwd_exam_paper_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
       where dt >= date_sub('$do_date', recent_days - 1)
         and dt <= '$do_date'
       group by recent_days, course_id) t
          left join (select id,
                            course_name
                     from ${APP}.dim_course_full
                     where dt = '$do_date') t1
                    on t.course_id = t1.id
);
  "

ads_exam_score_stats_by_paper="
insert overwrite table ${APP}.ads_exam_score_stats_by_paper
select * from ${APP}.ads_exam_score_stats_by_paper
union
select '$do_date',
       recent_days,
       paper_id,
       paper_title,
       sum(`if`(score >= 60,1,0)),
       sum(`if`(score < 60,1,0))
from dwd_exam_paper_inc lateral view explode(array(1,7,30)) tmp as recent_days
where dt >= date_sub('$do_date',recent_days - 1 ) and dt <= '$do_date'
group by recent_days,paper_id,paper_title;
"

ads_question_right_rate="
insert overwrite table ${APP}.ads_question_right_rate
select * from ${APP}.ads_question_right_rate
union
select '$do_date',
       recent_days,
       question_id,
       cast(sum(sum_right_title)/sum(sum_title) as decimal(16,2))
from ${APP}.dws_exam_title_question_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt >= date_sub('$do_date',recent_days - 1) and dt <= '$do_date'
group by recent_days,question_id;"

ads_play_stats_by_chapter="
insert overwrite table ${APP}.ads_play_stats_by_chapter
select
    *
from ${APP}.ads_play_stats_by_chapter
union
select
    dt, recent_days,chapter_id,chapter_name, video_play_cnt, avg_sec, user_count
from
(select
    '$do_date' dt,
    recent_days,
    chapter_id,
    count(distinct session_id) video_play_cnt,
    cast(sum(play_sec)/count(distinct user_id) as decimal(16,2)) avg_sec,
    count(distinct user_id) user_count
from dwd_video_play_inc
lateral view explode(array(1,7,30)) tmp as recent_days
where dt>date_sub('$do_date',recent_days)
group by recent_days,chapter_id) t1
join (
    select
        id,
        chapter_name
    from ${APP}.dim_chapter_full
    where dt='$do_date'
) t2 on t1.chapter_id=t2.id;
"

ads_play_stats_by_course="
insert overwrite table ${APP}.ads_play_stats_by_course
select
    *
from ${APP}.ads_play_stats_by_course
union
select
    dt, recent_days,t1.course_id,course_name, video_play_cnt, avg_sec, user_count
from
    (select
         '$do_date' dt,
         recent_days,
         course_id,
         count(distinct session_id) video_play_cnt,
         cast(sum(play_sec)/count(distinct user_id) as decimal(16,2)) avg_sec,
         count(distinct user_id) user_count
     from dwd_video_play_inc
              lateral view explode(array(1,7,30)) tmp as recent_days
     where dt>date_sub('$do_date',recent_days)
     group by recent_days,course_id) t1
        join (
        select
            course_id,
            course_name
        from ${APP}.dim_chapter_full
        where dt='$do_date'
    ) t2 on t1.course_id=t2.course_id;
"

ads_video_course_over="
insert overwrite table ${APP}.ads_video_course_over
select *
from ${APP}.ads_video_course_over
union
select '$do_date',
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
                  from ${APP}.dws_video_chapter_user_play_td
                  where dt = '$do_date'
                    and over_data = '$do_date'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from ${APP}.dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num
group by course_id, course_name
union
select '$do_date',
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
                  from ${APP}.dws_video_chapter_user_play_td
                  where dt = '$do_date'
                    and over_data >= date_add('$do_date', -6)
                    and over_data <= '$do_date'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from ${APP}.dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num
group by course_id, course_name
union
select '$do_date',
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
                  from ${APP}.dws_video_chapter_user_play_td
                  where dt = '$do_date'
                    and over_data >= date_add('$do_date', -29)
                    and over_data <= '$do_date'
                  group by user_id, course_id, course_name
              ) t1
                  left join (
             select course_id,
                    count(*) chapter_num
             from ${APP}.dim_chapter_full
             group by course_id) t2 on t1.course_id = t2.course_id
     ) t3
where chapter_num = chapter_user_num
group by course_id, course_name;
"

ads_trade_user="
insert overwrite table ${APP}.ads_trade_user
select * from ${APP}.ads_trade_user
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
         from ${APP}.dws_video_chapter_user_play_td lateral view explode(`array`(1,7,30)) tmp as days
         where dt='$do_date' and over_data=date_sub('$do_date',days-1)
         group by user_id,course_id,dt,days
     )t1
         left join (
    select course_id,
           count(id) chapter_cn
    from ${APP}.dim_chapter_full
    where dt='$do_date'
    group by course_id
)t2 on t1.course_id=t2.course_id
where t1.chapter_cn=t2.chapter_cn
group by dt, days;
"

ads_course_avg_chapter="
insert overwrite table ${APP}.ads_course_avg_chapter
select * from ${APP}.ads_course_avg_chapter
union
select '$do_date' dt,
       days retention_day,
       count(*)/count(distinct user_id) avg_chapter
from ${APP}.dws_video_chapter_user_play_td lateral view explode(`array`(1,7,30)) tmp as days
where dt='$do_date' and over_data >= date_sub('$do_date',days-1)
group by days;"


case $1 in
    "ads_traffic_stats_by_source" )
        hive -e "$ads_traffic_stats_by_source"
    ;;
    "ads_page_path" )
        hive -e "$ads_page_path"
    ;;
    "ads_order_amount_by_source" )
        hive -e "$ads_order_amount_by_source"
    ;;
    "ads_user_change" )
        hive -e "$ads_user_change"
    ;;
    "ads_user_retention" )
        hive -e "$ads_user_retention"
    ;;
    "ads_user_stats" )
        hive -e "$ads_user_stats"
    ;;
    "ads_user_action" )
        hive -e "$ads_user_action"
    ;;
    "ads_order_stats_by_add" )
        hive -e "$ads_order_stats_by_add"
    ;;
    "ads_order_stats_by_age" )
        hive -e "$ads_order_stats_by_age"
    ;;
    "ads_order_stats_by_category" )
        hive -e "$ads_order_stats_by_category"
    ;;
    "ads_order_stats_by_subject" )
        hive -e "$ads_order_stats_by_subject"
    ;;
    "ads_order_stats_by_course" )
        hive -e "$ads_order_stats_by_course"
    ;;
    "ads_review_stats_by_course" )
        hive -e "$ads_review_stats_by_course"
    ;;
    "ads_first_play_stats_by_category" )
        hive -e "$ads_first_play_stats_by_category"
    ;;
    "ads_first_play_stats_by_subject" )
        hive -e "$ads_first_play_stats_by_subject"
    ;;
    "ads_first_play_stats_by_course" )
        hive -e "$ads_first_play_stats_by_course"
    ;;
    "ads_order_all" )
        hive -e "$ads_order_all"
    ;;
    "ads_order_by_province" )
        hive -e "$ads_order_by_province"
   ;;
   "ads_exam_stats_by_paper" )
        hive -e "$ads_exam_stats_by_paper"
   ;;
   "ads_exam_course_stats_by_paper" )
           hive -e "$ads_exam_course_stats_by_paper"
  ;;
  "ads_exam_score_stats_by_paper" )
          hive -e "$ads_exam_score_stats_by_paper"
  ;;
  "ads_question_right_rate" )
          hive -e "$ads_question_right_rate"
  ;;
  "ads_play_stats_by_chapter" )
          hive -e "$ads_play_stats_by_chapter"
  ;;
  "ads_play_stats_by_course" )
          hive -e "$ads_play_stats_by_course"
  ;;
  "ads_video_course_over" )
          hive -e "$ads_video_course_over"
  ;;
  "ads_trade_user" )
            hive -e "$ads_video_course_over"
  ;;
  "ads_course_avg_chapter" )
            hive -e "$ads_video_course_over"
  ;;
    "all" )
        hive -e "$ads_traffic_stats_by_source$ads_page_path$ads_order_amount_by_source$ads_user_change$ads_user_retention$ads_user_stats$ads_user_action$ads_order_stats_by_add$ads_order_stats_by_age$ads_order_stats_by_category$ads_order_stats_by_subject$ads_order_stats_by_course$ads_review_stats_by_course$ads_first_play_stats_by_category$ads_first_play_stats_by_subject$ads_first_play_stats_by_course$ads_order_all$ads_order_by_province$ads_exam_stats_by_paper$ads_exam_course_stats_by_paper$ads_exam_score_stats_by_paper$ads_question_right_rate$ads_play_stats_by_chapter$ads_play_stats_by_course$ads_video_course_over$ads_trade_user$ads_course_avg_chapter"
    ;;
esac
