create database db_hive4;
use db_hive4;

drop table if exists live_events;
create table if not exists live_events
(
    user_id      int comment '用户id',
    live_id      int comment '直播id',
    in_datetime  string comment '进入直播间时间',
    out_datetime string comment '离开直播间时间'
)
    comment '直播间访问记录';

INSERT overwrite table live_events
VALUES (100, 1, '2021-12-01 19:00:00', '2021-12-01 19:28:00'),
       (100, 1, '2021-12-01 19:30:00', '2021-12-01 19:53:00'),
       (100, 2, '2021-12-01 21:01:00', '2021-12-01 22:00:00'),
       (101, 1, '2021-12-01 19:05:00', '2021-12-01 20:55:00'),
       (101, 2, '2021-12-01 21:05:00', '2021-12-01 21:58:00'),
       (102, 1, '2021-12-01 19:10:00', '2021-12-01 19:25:00'),
       (102, 2, '2021-12-01 19:55:00', '2021-12-01 21:00:00'),
       (102, 3, '2021-12-01 21:05:00', '2021-12-01 22:05:00'),
       (104, 1, '2021-12-01 19:00:00', '2021-12-01 20:59:00'),
       (104, 2, '2021-12-01 21:57:00', '2021-12-01 22:56:00'),
       (105, 2, '2021-12-01 19:10:00', '2021-12-01 19:18:00'),
       (106, 3, '2021-12-01 19:01:00', '2021-12-01 21:10:00');



drop table if exists page_view_events;
create table if not exists page_view_events
(
    user_id        int comment '用户id',
    page_id        string comment '页面id',
    view_timestamp bigint comment '访问时间戳'
)
    comment '页面访问记录';


insert overwrite table page_view_events
values (100, 'home', 1659950435),
       (100, 'good_search', 1659950446),
       (100, 'good_list', 1659950457),
       (100, 'home', 1659950541),
       (100, 'good_detail', 1659950552),
       (100, 'cart', 1659950563),
       (101, 'home', 1659950435),
       (101, 'good_search', 1659950446),
       (101, 'good_list', 1659950457),
       (101, 'home', 1659950541),
       (101, 'good_detail', 1659950552),
       (101, 'cart', 1659950563),
       (102, 'home', 1659950435),
       (102, 'good_search', 1659950446),
       (102, 'good_list', 1659950457),
       (103, 'home', 1659950541),
       (103, 'good_detail', 1659950552),
       (103, 'cart', 1659950563);




drop table if exists login_events;
create table if not exists login_events
(
    user_id        int comment '用户id',
    login_datetime string comment '登录时间'
)
    comment '直播间访问记录';


INSERT overwrite table login_events
VALUES (100, '2021-12-01 19:00:00'),
       (100, '2021-12-01 19:30:00'),
       (100, '2021-12-02 21:01:00'),
       (100, '2021-12-03 11:01:00'),
       (101, '2021-12-01 19:05:00'),
       (101, '2021-12-01 21:05:00'),
       (101, '2021-12-03 21:05:00'),
       (101, '2021-12-05 15:05:00'),
       (101, '2021-12-06 19:05:00'),
       (102, '2021-12-01 19:55:00'),
       (102, '2021-12-01 21:05:00'),
       (102, '2021-12-02 21:57:00'),
       (102, '2021-12-03 19:10:00'),
       (104, '2021-12-04 21:57:00'),
       (104, '2021-12-02 22:57:00'),
       (105, '2021-12-01 10:01:00');



drop table if exists promotion_info;
create table promotion_info
(
    promotion_id string comment '优惠活动id',
    brand        string comment '优惠品牌',
    start_date   string comment '优惠活动开始日期',
    end_date     string comment '优惠活动结束日期'
) comment '各品牌活动周期表';


insert overwrite table promotion_info
values (1, 'oppo', '2021-06-05', '2021-06-09'),
       (2, 'oppo', '2021-06-11', '2021-06-21'),
       (3, 'vivo', '2021-06-05', '2021-06-15'),
       (4, 'vivo', '2021-06-09', '2021-06-21'),
       (5, 'redmi', '2021-06-05', '2021-06-21'),
       (6, 'redmi', '2021-06-09', '2021-06-15'),
       (7, 'redmi', '2021-06-17', '2021-06-26'),
       (8, 'huawei', '2021-06-05', '2021-06-26'),
       (9, 'huawei', '2021-06-09', '2021-06-15'),
       (10, 'huawei', '2021-06-17', '2021-06-21');
