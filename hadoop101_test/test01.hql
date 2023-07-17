set mapreduce.framework.name=local;

show databases;
use default;
show tables;
select *
from test1;

import table test3 from '/origin_data/gmall/db/activity_info_full/2022-06-08/'
;
drop table user_info;

create table if not exists user_info
(
    id           bigint comment '编号',
    login_name   string comment '用户名称',
    nick_name    string comment '用户昵称',
    passwd       string comment '用户密码',
    name         string comment '用户姓名',
    phone_num    string comment '手机号',
    email        string comment '邮箱',
    head_img     string comment '头像',
    user_level   string comment '用户级别',
    birthday     string comment '用户生日',
    gender       string comment '性别 M男,F女',
    create_time  timestamp comment '创建时间',
    operate_time timestamp comment '修改时间',
    status       string comment '状态'
)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    tblproperties ('serialization.encoding' = 'UTF-8', 'compression.type' = 'gzip');


load data inpath 'hdfs:///origin_data/gmall/db/user_info_inc/2022-06-09/db.1686124236343.gz' into table user_info;