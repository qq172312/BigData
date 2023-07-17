//开启自动转换为本地模式
set hive.exec.mode.local.auto=true;

//hive集合类型

//1、struct,array,map
use db_hive1;
create table collection
(
    friends  array<string>,
    students map<string,int>,
    info     struct<street:string,city:string,user_id:int>
)
    row format delimited fields terminated by ','
        collection items terminated by '_'
        map keys terminated by ':'
        lines terminated by '\n';

//数据格式
//two,twof1_twof2,twostu1:22_twostu2:24,hong fu yuan_beijign_'434533@qq.com'


//json 格式
create table collection
(
    friends  array<string>,
    students map<string,int>,
    info     struct<street:string,city:string,user_id:int>
)
row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'

//格式
//{"name":"dasongsong","friends":["bingbing","lili"],"students":{"xiaohaihai":18,"xiaoyangyang":16},"address":{"street":"hui long guan","city":"beijing","postal_code":10010}}
