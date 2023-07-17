//1、 同时在线人数问题

//3s
select live_id,
       max(sum) max_user_events
from (select live_id,
             sum(flag) over (partition by live_id order by io_time) sum
      from (select user_id,
                   live_id,
                   in_datetime io_time,
                   1           flag
            from live_events
            union
            select user_id,
                   live_id,
                   out_datetime io_time,
                   -1           flag
            from live_events) t1) t2
group by live_id;


//第2题 会话划分问题

//2.4s
select t1.user_id,
       t1.page_id,
       t1.view_timestamp,
       concat(user_id, '-', sum(flag) over (partition by user_id order by view_timestamp)) session_id
from (select *,
             `if`(view_timestamp - lag(view_timestamp, 1, 0) over (partition by user_id order by view_timestamp) <= 60,
                  0, 1) flag
      from page_view_events) t1;


//参考 2.5s
select user_id,
       page_id,
       view_timestamp,
       concat(user_id, '-', sum(session_start_point) over (partition by user_id order by view_timestamp)) session_id
from (select user_id,
             page_id,
             view_timestamp,
             if(view_timestamp - lagts >= 60, 1, 0) session_start_point
      from (select user_id,
                   page_id,
                   view_timestamp,
                   lag(view_timestamp, 1, 0) over (partition by user_id order by view_timestamp) lagts
            from page_view_events) t1) t2;


//第3题 间断连续登录用户问题
select t4.user_id,
       max(recent_days) max_day_count
from (select user_id,
             datediff(max(login_datetime), min(login_datetime)) + 1 recent_days
      from (select t2.user_id,
                   login_datetime,
                   last,
                   concat(user_id, '-', flag) user_flag
            from (select user_id,
                         login_datetime,
                         last,
                         sum(`if`(datediff(login_datetime, last) > 2, 1, 0))
                             over (partition by user_id order by login_datetime) flag
                  from (select user_id,
                               substr(login_datetime, 1, 10)                                                   login_datetime,
                               lag(substr(login_datetime, 1, 10), 1, '1970-09-09') over (partition by user_id) last
                        from login_events
                        group by user_id, substr(login_datetime, 1, 10)) t1) t2) t3
      group by user_id, user_flag) t4
group by user_id;


//类似 --flag是判断其是否在一个连续的分组中
select
    user_id,
    max(recent_days) max
from (select
    user_id,
    datediff(max(login_datetime),min(login_datetime))+1 recent_days
from (select
  user_id,
  login_datetime,
  last,
  concat(user_id,'-',flag) user_flag
from (select
    user_id,
    login_datetime,
    last,
    sum(`if`(datediff(login_datetime,last)>1,1,0)) over (partition by user_id order by login_datetime) flag
from (select user_id,
             substr(login_datetime, 1, 10)                                               login_datetime,
             lag(substr(login_datetime, 1, 10), 1, '1970-09-09') over (partition by user_id) last
      from login_events
      group by user_id, substr(login_datetime, 1, 10)
      ) t1)
    t2
)t3
group by user_id,user_flag) t4
group by user_id;


//第4题 日期交叉问题

//3s
select
    brand,
    sum(day+1) day
from (select
    brand,
    datediff(max(dt),min(dt)) day
from (select
    brand,
    dt,
    concat(brand,'-',sum(`if`(sum_flag=1 and flag=1,1,0)) over (partition by brand order by dt)) group_id
from (select *,
       sum(flag) over (partition by brand rows between unbounded preceding and current row ) sum_flag
from (select
    brand,
    start_date `dt`,
    1 flag
from promotion_info
union
select
    brand,
    end_date `dt`,
    -1 flag
from promotion_info) t1) t2) t3
group by brand,group_id) t4
group by brand;


//参考2.6s
select
    brand,
    sum(datediff(end_date,start_date)+1) promotion_day_count
from
    (
        select
            brand,
            max_end_date,
            if(max_end_date is null or start_date>max_end_date,start_date,date_add(max_end_date,1)) start_date,
            end_date
        from
            (
                select
                    brand,
                    start_date,
                    end_date,
                    max(end_date) over(partition by brand order by start_date rows between unbounded preceding and 1 preceding) max_end_date
                from promotion_info
            )t1
    )t2
where end_date>start_date
group by brand;



//第10题  员工在职人数问题
set mapreduce.framework.name;
