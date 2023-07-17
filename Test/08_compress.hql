//1、压缩
create database tuning_hive;

//1.1 textfile

/*
 在执行往表中导入数据的SQL语句时，用户需设置以下参数，来保证写入表中的数据是被压缩的。
 */

--SQL语句的最终输出结果是否压缩
set hive.exec.compress.output=true;
--输出结果的压缩格式（以下示例为snappy）
set mapreduce.output.fileoutputformat.compress.codec =org.apache.hadoop.io.compress.SnappyCodec;

//1.2 orc

-- /需在建表语句中声明压缩格式如下：
drop table orc_table;
create table orc_table(
    id int,
    name string
)
stored as orc
tblproperties ('orc.compress'='snappy');

//1.3 parquet同上

//2 MR压缩

//2.1 单个mR压缩

--开启MapReduce中间数据压缩功能
set mapreduce.map.output.compress=true;
--设置MapReduce中间数据数据的压缩方式（以下示例为snappy）
set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;

//2.2 单条sql语句的中间结果进行压缩(两个mr之间的临时数据)

--是否对两个MR之间的临时数据进行压缩
set hive.exec.compress.intermediate=true;
--压缩格式（以下示例为snappy）
set hive.intermediate.compression.codec= org.apache.hadoop.io.compress.SnappyCodec;



/*
    不同文件性能测试
    存储性能：orc(7.6m)>parquet(13m)>textfile(18m)
    查询性能：orc=parquet(26s)>textfile(34s)
    统计的时候,列式存储优势更明显。
*/
----------1.textfile
use tuning_hive;

create table log_text (
                          track_time string,
                          url string,
                          session_id string,
                          referer string,
                          ip string,
                          end_user_id string,
                          city_id string
)
    row format delimited fields terminated by '\t'
    stored as textfile;

load data local inpath '/opt/module/hive-3.1.3/datas/log.data' into table log_text;


----------orc
create table log_orc(
                        track_time string,
                        url string,
                        session_id string,
                        referer string,
                        ip string,
                        end_user_id string,
                        city_id string
)
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties("orc.compress"="NONE");

insert into table log_orc select * from log_text;


----------parquet
create table log_parquet(
                            track_time string,
                            url string,
                            session_id string,
                            referer string,
                            ip string,
                            end_user_id string,
                            city_id string
)
    row format delimited fields terminated by '\t'
    stored as parquet ;

insert into table log_parquet select * from log_text;







