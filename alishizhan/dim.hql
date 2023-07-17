
/*

    DIM层的设计依据是维度建模理论，该层存储维度模型的维度表。
（2）DIM层的数据存储格式为orc列式存储+snappy压缩。
（3）DIM层表名的命名规范为dim_表名_全量表或者拉链表标识（full/zip）。

 */

//1、课程维度表(课程信息表，分类表，科目表)
DROP TABLE IF EXISTS dim_course_full;
CREATE EXTERNAL TABLE dim_course_full
(
    `id` string COMMENT '课程id' ,
    `course_name` string    COMMENT '课程名称' ,
    `subject_id` string    COMMENT '学科id' ,
    `subject_name` string   COMMENT '科目名称' ,
    `category_id` string    COMMENT '分类' ,
    `category_name`     STRING COMMENT '分类名称',
    `teacher`               STRING COMMENT '讲师名称',
    `publisher_id`             STRING COMMENT '发布者id',
    `chapter_num`         BIGINT COMMENT '章节数',
    `origin_price` DECIMAL(16,2)    COMMENT '价格' ,
    `reduce_amount` DECIMAL(16,2)    COMMENT '优惠金额' ,
    `actual_price` DECIMAL(16,2)    COMMENT '实际价格' ,
    `create_time` string    COMMENT '创建时间'
) COMMENT '课程维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dim/dim_course_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

//--首日（每日）装载

insert overwrite  table dim_course_full partition (dt = '2022-06-08')
select course.id           ,--        STRING COMMENT '课程id',
       `course_name`     ,--     STRING COMMENT '课程名称',
       `subject_id`      ,--       STRING COMMENT '学科id',
       `subject_name`    ,--         STRING COMMENT '科目名称',
       `category_id`     ,--          STRING COMMENT '分类',
       `category_name`   ,--           BOOLEAN COMMENT '分类名称',
       `teacher`         ,--      STRING COMMENT '讲师名称',
       `publisher_id`    ,--         STRING COMMENT '发布者id',
       `chapter_num`     ,--    BIGINT COMMENT '章节数',
       `origin_price`    ,--   DECIMAL(16,2) COMMENT '价格',
       `reduce_amount`   ,--      DECIMAL(16,2) COMMENT '优惠金额',
       `actual_price`    ,--   DECIMAL(16,2) COMMENT '实际价格',
       `create_time`     --  STRING COMMENT '创建时间'
from (
         select id,
                course_name,
                subject_id,
                teacher,
                publisher_id,
                chapter_num,
                origin_price,
                reduce_amount,
                actual_price,
                create_time
         from ods_course_info_full
         where dt = '2022-06-08'
     )course
         left join (
    select id,
           subject_name,
           category_id
    from ods_base_subject_info_full
    where dt = '2022-06-08'
) sub on course.subject_id = sub.id
         left join (
    select id,
           category_name
    from ods_base_category_info_full
    where dt = '2022-06-08'
)cat on sub.category_id = cat.id;


//2、章节维度表(用户章节观看表，章节信息表)
DROP TABLE IF EXISTS dim_chapter_full;
CREATE EXTERNAL TABLE dim_chapter_full
(
    `id` string  COMMENT '编号' ,
    `chapter_name` string    COMMENT '章节名称' ,
    `course_id` string    COMMENT '课程id' ,
    `course_name` string    COMMENT '课程名称' ,
    `video_id` string    COMMENT '视频id' ,
    `video_name` string    COMMENT '视频名称' ,
    `during_sec` BIGINT    COMMENT '时长' ,
    `publisher_id` string    COMMENT '发布者id' ,
    `create_time` string    COMMENT '创建时间'
) COMMENT '章节维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dim/dim_chapter_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


--首日（每日）装载

insert overwrite table dim_chapter_full partition (dt = '2022-06-08')
select     chap.`id`      ,--              STRING COMMENT '章节ID',
           `chapter_name`   ,--       STRING COMMENT '章节名称',
           `course_id`      ,--   STRING COMMENT '课程id',
           `course_name`    ,--    STRING COMMENT '课程名称',
           `video_id`       ,-- STRING COMMENT '视频id',
           `video_name`     ,-- string COMMENT '视频名称'
           during_sec,      -- ,
           `publisher_id`   ,-- string COMMENT '发布者id' ,
           `create_time`    -- string COMMENT '创建时间'
from (
         select id,
                chapter_name,
                course_id,
                video_id,
                publisher_id,
                create_time
         from ods_chapter_info_full
         where dt = '2022-06-08'
     )chap
         left join (
    select id,
           course_name
    from ods_course_info_full
    where dt = '2022-06-08'
)cou on chap.course_id = cou.id
         left join (
    select id,
           video_name,
           during_sec
    from ods_video_info_full
    where dt = '2022-06-08'
)vi on chap.video_id = vi.id;


//3、地区维度表（地区表）


DROP TABLE IF EXISTS dim_province_full;
CREATE EXTERNAL TABLE dim_province_full
(
    `id`              STRING COMMENT '编号',
    `name`            STRING COMMENT '省份名称',
    `region_id`      STRING COMMENT '地区ID',
    `area_code`      STRING COMMENT '行政区位码',
    `iso_code`      STRING COMMENT '国际编码',
    `iso_3166_2`      STRING COMMENT 'ISO3166 编码'
) COMMENT '章节维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dim/dim_province_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dim_province_full partition (dt = '2022-06-08')
select  `id`  ,--            STRING COMMENT '省份ID',
        `name`       ,--   STRING COMMENT '省份名称',
        `region_id`  ,--   STRING COMMENT '大区id',
        `area_code`    ,--  STRING COMMENT '行政区位码',
        `iso_code`     ,-- STRING COMMENT '国际编码',
        `iso_3166_2`
from ods_base_province_full
where dt = '2022-06-08';

//4、问题维度表（试卷问题关联表）
DROP TABLE IF EXISTS dim_question_full;
CREATE EXTERNAL TABLE dim_question_full
(
    `id` string COMMENT '编号' ,
    `course_id` string    COMMENT '课程id' ,
    `course_name` string    COMMENT '课程名称' ,
    `question_txt` string    COMMENT '题目内容' ,
    `question_type` string    COMMENT '题目类型' ,
    `create_time` string    COMMENT '创建时间'
) COMMENT '章节维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dim/dim_question_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日（每日）装载

insert overwrite table dim_question_full partition (dt = '2022-06-08')
select    que.`id`       ,--             STRING COMMENT '问题ID',
          `course_id`       ,-- STRING COMMENT '课程id',
          `course_name`     ,-- string COMMENT '视频名称' ,
          `question_txt`    ,--      STRING COMMENT '题目内容',
          `question_type`   ,-- string COMMENT '题目类型' ,
          `create_time`     --string COMMENT '创建时间'
from (
         select id,
                question_txt,
                chapter_id,
                course_id,
                question_type,
                create_time
         from ods_test_question_info_full
         where dt = '2022-06-08'
     )que join (
    select id,
           course_name
    from ods_course_info_full
    where dt = '2022-06-08'
)cou on que.course_id=cou.id;
//5、用户维度（用户表）

DROP TABLE IF EXISTS dim_user_zip;
CREATE EXTERNAL TABLE dim_user_zip
(
    `id`           STRING COMMENT '用户ID',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/edu/dim/dim_user_zip/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

//首日装载
insert overwrite table dim_user_zip partition (dt='9999-12-31')
select
    data.id,
    concat(substr(data.real_name,1,1),'*') name,
    if(data.phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
       concat(substr(data.phone_num, 1, 3), '*'), null) phone_num,
    if(data.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
       concat('*@', split(data.email, '@')[1]), null)   email,
    data.user_level,
    data.birthday,
    data.gender,
    data.create_time,
    data.operate_time,
    '2022-06-08'                                        start_date,
    '9999-12-31'                                        end_date
from ods_user_info_inc
where dt='2022-06-08' and type='bootstrap-insert';

--每日装载
set hive.exec.dynamic.partition.mode= nonstrict;
insert overwrite table dim_user_zip partition (dt)
select `id`          ,-- STRING COMMENT '用户ID',
       `real_name`   ,--      STRING COMMENT '用户姓名',
       `phone_num`   ,-- STRING COMMENT '手机号码',
       `email`       ,-- STRING COMMENT '邮箱',
       `user_level`  ,-- STRING COMMENT '用户等级',
       `birthday`    ,-- STRING COMMENT '生日',
       `gender`      ,-- STRING COMMENT '性别',
       `create_time` ,-- STRING COMMENT '创建时间',
       `operate_time`,-- STRING COMMENT '操作时间',
       start_date,
       `if`(rn = 2,date_sub('2022-06-09',1),end_date) end_date,
       `if`(rn = 1,'9999-12-31',date_sub('2022-06-09',1)) dt
from(
        select  `id`          ,-- STRING COMMENT '用户ID',
                `real_name`   ,--      STRING COMMENT '用户姓名',
                `phone_num`   ,-- STRING COMMENT '手机号码',
                `email`       ,-- STRING COMMENT '邮箱',
                `user_level`  ,-- STRING COMMENT '用户等级',
                `birthday`    ,-- STRING COMMENT '生日',
                `gender`      ,-- STRING COMMENT '性别',
                `create_time` ,-- STRING COMMENT '创建时间',
                `operate_time`,-- STRING COMMENT '操作时间',
                start_date,
                end_date,
                row_number() over (partition by id order by start_date desc) rn
        from (
                 select  `id`          ,-- STRING COMMENT '用户ID',
                         `real_name`   ,--      STRING COMMENT '用户姓名',
                         `phone_num`   ,-- STRING COMMENT '手机号码',
                         `email`       ,-- STRING COMMENT '邮箱',
                         `user_level`  ,-- STRING COMMENT '用户等级',
                         `birthday`    ,-- STRING COMMENT '生日',
                         `gender`      ,-- STRING COMMENT '性别',
                         `create_time` ,-- STRING COMMENT '创建时间',
                         `operate_time`,-- STRING COMMENT '操作时间',
                         start_date,
                         end_date
                 from dim_user_zip
                 where dt = '9999-12-31'
                 union
                 select  `id`          ,-- STRING COMMENT '用户ID',
                         `real_name`   ,--      STRING COMMENT '用户姓名',
                         `phone_num`   ,-- STRING COMMENT '手机号码',
                         `email`       ,-- STRING COMMENT '邮箱',
                         `user_level`  ,-- STRING COMMENT '用户等级',
                         `birthday`    ,-- STRING COMMENT '生日',
                         `gender`      ,-- STRING COMMENT '性别',
                         `create_time` ,-- STRING COMMENT '创建时间',
                         `operate_time`,-- STRING COMMENT '操作时间',
                         '2022-06-09' start_date,
                         '9999-12-31' end_date
                 from (
                          select data.`id`          ,-- STRING COMMENT '用户ID',
                                 data.`real_name`   ,--      STRING COMMENT '用户姓名',
                                 data.`phone_num`   ,-- STRING COMMENT '手机号码',
                                 data.`email`       ,-- STRING COMMENT '邮箱',
                                 data.`user_level`  ,-- STRING COMMENT '用户等级',
                                 data.`birthday`    ,-- STRING COMMENT '生日',
                                 data.`gender`      ,-- STRING COMMENT '性别',
                                 data.`create_time` ,-- STRING COMMENT '创建时间',
                                 data.`operate_time`,-- STRING COMMENT '操作时间',
                                 row_number() over (partition by date.id order by ts desc) rn
                          from ods_user_info_inc
                          where dt = '2022-06-09'
                      )t1
                 where rn = 1
             )t2
    )t3