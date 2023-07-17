
//1、ods日志表
DROP TABLE IF EXISTS ods_log_inc;
CREATE EXTERNAL TABLE ods_log_inc
(
    `common` STRUCT<
        ar :STRING,
        ba :STRING,
        ch :STRING,
        is_new :STRING,
        md :STRING,
        mid :STRING,
        os :STRING,
        sc :STRING,
        sid :STRING,
        uid :STRING,
        vc :STRING> COMMENT '公共信息',
    `page` STRUCT<during_time :STRING,
                  item :STRING,
                  item_type :STRING,
                  last_page_id :STRING,
                  page_id :STRING> COMMENT '页面信息',
    `actions` ARRAY<STRUCT<action_id:STRING,
                           item:STRING,
                           item_type:STRING,
                           ts:BIGINT>> COMMENT '动作信息',
    `displays` ARRAY<STRUCT<display_type :STRING,
                            item :STRING,
                            item_type :STRING,
                            `order` :STRING,
                            pos_id :STRING>
                            > COMMENT '曝光信息',
    `start` STRUCT<entry :STRING,
                   first_open :BIGINT,
                   loading_time :BIGINT,
                   open_ad_id :BIGINT,
                   open_ad_ms :BIGINT,
                   open_ad_skip_ms :BIGINT> COMMENT '启动信息',
    `appVideo` STRUCT<play_sec :BIGINT,
                      position_sec :BIGINT,
                      video_id :string> COMMENT '启动信息',
    `err` STRUCT<error_code:BIGINT,
                 msg:STRING> COMMENT '错误信息',
    `ts` BIGINT  COMMENT '时间戳'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_log_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

//2、业务表
//2.1 商品分类表（全量表）

DROP TABLE IF EXISTS ods_base_category_info_full;
CREATE EXTERNAL TABLE ods_base_category_info_full
(
    `id`              STRING COMMENT '编号',
    `category_name`     STRING COMMENT '分类名称',
    `create_time`    STRING COMMENT '创建时间',
    `update_time`    STRING COMMENT '更新时间',
    `deleted`   STRING COMMENT '是否删除'
) COMMENT '分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_base_category_info_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

//2.2 省份表(全量)

DROP TABLE IF EXISTS ods_base_province_full;
CREATE EXTERNAL TABLE ods_base_province_full
(
    `id`              STRING COMMENT '编号',
    `name`            STRING COMMENT '省份名称',
    `region_id`      STRING COMMENT '地区ID',
    `area_code`      STRING COMMENT '行政区位码',
    `iso_code`      STRING COMMENT '国际编码',
    `iso_3166_2`      STRING COMMENT 'ISO3166 编码'
) COMMENT '省份表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_base_province_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.3 来源表
DROP TABLE IF EXISTS ods_base_source_full;
CREATE EXTERNAL TABLE ods_base_source_full
(
    `id`               STRING COMMENT '引流来源id',
    `source_site`             STRING COMMENT '引流来源名称',
    `source_url`    STRING COMMENT '引流来源链接'
) COMMENT '一级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_base_source_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



//2.4科目表

DROP TABLE IF EXISTS ods_base_subject_info_full;
CREATE TABLE ods_base_subject_info_full(
                                  `id` string  COMMENT '编号' ,
                                  `subject_name` string   COMMENT '科目名称' ,
                                  `category_id` string    COMMENT '分类' ,
                                  `create_time` string    COMMENT '创建时间' ,
                                  `update_time` string    COMMENT '更新时间' ,
                                  `deleted` string    COMMENT '是否删除'
)  COMMENT  '学科'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_base_subject_info_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



//2.5 加购表

DROP TABLE IF EXISTS ods_cart_info_full;
CREATE TABLE ods_cart_info_full(
                          `id` string  COMMENT '编号' ,
                          `user_id` string    COMMENT '用户id' ,
                          `course_id` string    COMMENT '课程id' ,
                          `course_name` string    COMMENT 'sku名称 (冗余)' ,
                          `cart_price` DECIMAL(16,2)    COMMENT '放入购物车时价格' ,
                          `img_url` string    COMMENT '图片文件' ,
                          `session_id` string    COMMENT '会话id' ,
                          `create_time` string    COMMENT '创建时间' ,
                          `update_time` string    COMMENT '修改时间' ,
                          `deleted` string    COMMENT '是否删除' ,
                          `sold` string    COMMENT '是否已售'
)  COMMENT '购物车表 用户登录系统时更新冗余'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_cart_info_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.6 章节表

DROP TABLE IF EXISTS ods_chapter_info_full;
CREATE TABLE ods_chapter_info_full(
                             `id` string  COMMENT '编号' ,
                             `chapter_name` string    COMMENT '章节名称' ,
                             `course_id` string    COMMENT '课程id' ,
                             `video_id` string    COMMENT '视频id' ,
                             `publisher_id` string    COMMENT '发布者id' ,
                             `is_free` string    COMMENT '是否免费' ,
                             `create_time` string    COMMENT '创建时间' ,
                             `deleted` string    COMMENT '是否删除' ,
                             `update_time` string    COMMENT '更新时间'
)  COMMENT '章节表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_chapter_info_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.7 课程信息表


DROP TABLE IF EXISTS ods_course_info_full;
CREATE TABLE ods_course_info_full(
                            `id` string COMMENT '编号' ,
                            `course_name` string    COMMENT '课程名称' ,
                            `course_slogan` string    COMMENT '课程标语' ,
                            `course_cover_url` string    COMMENT '课程封面' ,
                            `subject_id` string    COMMENT '学科id' ,
                            `teacher` string    COMMENT '讲师名称' ,
                            `publisher_id` string    COMMENT '发布者id' ,
                            `chapter_num` bigint   COMMENT '章节数' ,
                            `origin_price` DECIMAL(16,2)    COMMENT '价格' ,
                            `reduce_amount` DECIMAL(16,2)    COMMENT '优惠金额' ,
                            `actual_price` DECIMAL(16,2)    COMMENT '实际价格' ,
                            `course_introduce` string    COMMENT '课程介绍' ,
                            `create_time` string    COMMENT '创建时间' ,
                            `deleted` string    COMMENT '是否删除' ,
                            `update_time` string    COMMENT '更新时间'
)  COMMENT  '课程信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_course_info_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

//2.8 知识点表

DROP TABLE IF EXISTS ods_knowledge_point_full;
CREATE TABLE ods_knowledge_point_full(
                                `id` string  COMMENT '编号' ,
                                `point_txt` string    COMMENT '知识点 内容' ,
                                `point_level` string    COMMENT '知识点基本' ,
                                `course_id` string    COMMENT '课程id' ,
                                `chapter_id` string    COMMENT '章节id' ,
                                `create_time` string    COMMENT '创建时间' ,
                                `update_time` string    COMMENT '修改时间' ,
                                `publisher_id` string    COMMENT '发布者id' ,
                                `deleted` string    COMMENT '是否删除'
)  COMMENT '知识点表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_knowledge_point_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

//2.9 试卷表

DROP TABLE IF EXISTS ods_test_paper_full;
CREATE TABLE ods_test_paper_full(
                           `id` string COMMENT '编号' ,
                           `paper_title` string    COMMENT '试卷名称' ,
                           `course_id` string    COMMENT '课程id' ,
                           `create_time` string    COMMENT '创建时间' ,
                           `update_time` string    COMMENT '更新时间' ,
                           `publisher_id` string    COMMENT '发布者id' ,
                           `deleted` string    COMMENT '是否删除'
)  COMMENT '试卷表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_test_paper_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.10 试卷问题关联表

DROP TABLE IF EXISTS ods_test_paper_question_full;
CREATE TABLE ods_test_paper_question_full(
                                    `id` string COMMENT '编号' ,
                                    `paper_id` string    COMMENT '试卷id' ,
                                    `question_id` string    COMMENT '题目id' ,
                                    `score` DECIMAL(16,2)    COMMENT '得分' ,
                                    `create_time` string    COMMENT '创建时间' ,
                                    `deleted` string    COMMENT '是否删除' ,
                                    `publisher_id` string    COMMENT '发布者id'
)  COMMENT '试卷问题关联表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_test_paper_question_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.11 知识点与问题的关联

DROP TABLE IF EXISTS ods_test_point_question_full;
CREATE TABLE ods_test_point_question_full(
                                    `id` string  COMMENT '编号' ,
                                    `point_id` string    COMMENT '知识点id' ,
                                    `question_id` string    COMMENT '问题id' ,
                                    `create_time` string    COMMENT '创建时间' ,
                                    `publisher_id` string    COMMENT '发布者id' ,
                                    `deleted` string    COMMENT '是否删除'
)  COMMENT '知识点与问题的关联'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_test_point_question_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.12 问题信息表

DROP TABLE IF EXISTS ods_test_question_info_full;
CREATE TABLE ods_test_question_info_full(
                                   `id` string  COMMENT '编号' ,
                                   `question_txt` string    COMMENT '题目内容' ,
                                   `chapter_id` string   COMMENT '章节id' ,
                                   `course_id` string    COMMENT '课程id' ,
                                   `question_type` string    COMMENT '题目类型' ,
                                   `create_time` string    COMMENT '创建时间' ,
                                   `update_time` string    COMMENT '更新时间' ,
                                   `publisher_id` string    COMMENT '发布者id' ,
                                   `deleted` string    COMMENT '是否删除'
)  COMMENT '问题信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_test_question_info_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

//2.13 问题选项表


DROP TABLE IF EXISTS ods_test_question_option_full;
CREATE TABLE ods_test_question_option_full(
                                     `id` string  COMMENT '编号' ,
                                     `option_txt` string    COMMENT '选项内容' ,
                                     `question_id` string    COMMENT '题目id' ,
                                     `is_correct` string    COMMENT '是否正确' ,
                                     `create_time` string    COMMENT '创建时间' ,
                                     `update_time` string    COMMENT '更新时间' ,
                                     `deleted` string    COMMENT '是否删除'
)  COMMENT '问题选项表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_test_question_option_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.14 用户章节观看表

DROP TABLE IF EXISTS ods_user_chapter_process_full;
CREATE TABLE ods_user_chapter_process_full(
                                     `id` string  COMMENT '编号' ,
                                     `course_id` string    COMMENT '课程id' ,
                                     `chapter_id` string    COMMENT '章节id' ,
                                     `user_id` string    COMMENT '用户id' ,
                                     `position_sec` BIGINT    COMMENT '时长位置' ,
                                     `create_time` string    COMMENT '创建时间' ,
                                     `update_time` string    COMMENT '更新时间' ,
                                     `deleted` string    COMMENT '是否删除'
)  COMMENT '用户章节观看进度'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_user_chapter_process_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

//2.15 视频信息表

DROP TABLE IF EXISTS ods_video_info_full;
CREATE TABLE ods_video_info_full(
                           `id` string  COMMENT '编号' ,
                           `video_name` string    COMMENT '视频名称' ,
                           `during_sec` BIGINT    COMMENT '时长' ,
                           `video_status` string    COMMENT '状态 未上传，上传中，上传完' ,
                           `video_size` BIGINT    COMMENT '大小' ,
                           `video_url` string    COMMENT '视频存储路径' ,
                           `video_source_id` string    COMMENT '云端资源编号' ,
                           `version_id` string    COMMENT '版本号' ,
                           `chapter_id` string    COMMENT '章节id' ,
                           `course_id` string    COMMENT '课程id' ,
                           `publisher_id` string    COMMENT '发布者id' ,
                           `create_time` string    COMMENT '创建时间' ,
                           `update_time` string    COMMENT '更新时间' ,
                           `deleted` string    COMMENT '是否删除'
)  COMMENT '视频表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    NULL DEFINED AS ''
    LOCATION '/warehouse/edu/ods/ods_video_info_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.16 加购表(增量)

DROP TABLE IF EXISTS ods_cart_info_inc;
CREATE EXTERNAL TABLE ods_cart_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id` :string,
                  `user_id` : string   ,
                  `course_id`: string   ,
                  `course_name`: string    ,
                  `cart_price` :DECIMAL(16,2)   ,
                  `img_url` :string   ,
                  `session_id`: string   ,
                  `create_time` :string   ,
                  `update_time` :string   ,
                  `deleted`: string   ,
                  `sold`: string > comment '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '购物车增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_cart_info_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.17 收藏表(增量)


DROP TABLE IF EXISTS ods_favor_info_inc;
CREATE EXTERNAL TABLE ods_favor_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id` :string,
                  `course_id` :string  ,
                  `user_id` :string  ,
                  `create_time` :string  ,
                  `update_time` :string  ,
                  `deleted` :string
                  > comment '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '收藏增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_favor_info_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.18 章节评价表

DROP TABLE IF EXISTS ods_comment_info_inc;
CREATE EXTERNAL TABLE ods_comment_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id` :string,
                  `user_id` :string   ,
                  `chapter_id` :string   ,
                  `course_id` :string   ,
                  `comment_txt` :string   ,
                  `create_time` :string   ,
                  `deleted` :string >comment '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '章节评论增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_comment_info_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.19 订单明细表

DROP TABLE IF EXISTS ods_order_detail_inc;
CREATE EXTERNAL TABLE ods_order_detail_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id` :string,
                  `course_id` :string ,
                  `course_name` :string ,
                  `order_id` :string ,
                  `user_id` :string ,
                  `origin_amount` :DECIMAL(16,2) ,
                  `coupon_reduce` :DECIMAL(16,2)    ,
                  `final_amount` :DECIMAL(16,2) ,
                  `create_time` :string ,
                  `update_time` :string
                  > comment '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_order_detail_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

//2.20 订单表(增量表)
DROP TABLE IF EXISTS ods_order_info_inc;
CREATE EXTERNAL TABLE ods_order_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id` :string,
                  `user_id` :string,
                  `origin_amount` :DECIMAL(16,2),
                  `coupon_reduce` :DECIMAL(16,2),
                  `final_amount` :DECIMAL(16,2),
                  `order_status` :string,
                  `out_trade_no` :string,
                  `trade_body` :string,
                  `session_id` :string  ,
                  `province_id` :string,
                  `create_time` :string,
                  `expire_time` :string,
                  `update_time` :string
    > comment '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_order_info_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.21 支付表(增量表)
DROP TABLE IF EXISTS ods_payment_info_inc;
CREATE EXTERNAL TABLE ods_payment_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id` :string,
                  `out_trade_no` :string ,
                  `order_id` :string,
                  `alipay_trade_no` :string  ,
                  `total_amount` :DECIMAL(16,2),
                  `trade_body` :string,
                  `payment_type` :string,
                  `payment_status` :string,
                  `create_time` :string,
                  `update_time` :string,
                  `callback_content` :string,
                  `callback_time` :string
    > comment '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '支付增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_payment_info_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

//2.22 课程评价表

DROP TABLE IF EXISTS ods_review_info_inc;
CREATE EXTERNAL TABLE ods_review_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id` :string,
                  `user_id` :string ,
                  `course_id` :STRING ,
                  `review_txt` :string ,
                  `review_stars` :string,
                  `create_time` :string ,
                  `deleted` :string
    > comment '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '课程评价增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_review_info_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

//2.23 测验表

DROP TABLE IF EXISTS ods_test_exam_inc;
CREATE EXTERNAL TABLE ods_test_exam_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT< id :string,
                   paper_id :string ,
                   user_id :string ,
                   score :decimal(16,2),
                   duration_sec :bigint ,
                   create_time :string ,
                   submit_time :string ,
                   update_time :string ,
                   deleted :string
    > comment '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '试卷测试增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_test_exam_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.24 vip变化表(增量表)

DROP TABLE IF EXISTS ods_vip_change_detail_inc;
CREATE EXTERNAL TABLE ods_vip_change_detail_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT< id :string,
                   user_id :string,
                   from_vip :string ,
                   to_vip :string ,
                   create_time :string
    > comment '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT 'vip变化增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_vip_change_detail_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');


//2.25 用户表(增量表)
DROP TABLE IF EXISTS ods_user_info_inc;
CREATE EXTERNAL TABLE ods_user_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<  id :string,
                    login_name :string,
                    nick_name :string,
                    passwd :string,
                    real_name :string,
                    phone_num :string,
                    email :string,
                    head_img :string,
                    user_level :string,
                    birthday :string,
                    gender :string,
                    create_time :string,
                    operate_time :string,
                    status :string
    > comment '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '用户拉链表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_user_info_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

//2.26 问题测验表(增量表)

DROP TABLE IF EXISTS ods_test_exam_question_inc;
CREATE EXTERNAL TABLE ods_test_exam_question_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<  id :string,
                    exam_id :string,
                    paper_id :string,
                    question_id :string,
                    user_id :string,
                    answer :string,
                    is_correct :string,
                    score :decimal(16,2),
                    create_time :string,
                    update_time :string,
                    deleted :string
    > comment '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '试卷问题测试增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/edu/ods/ods_test_exam_question_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

show tables
;
msck repair table ods_base_province_full sync partitions