create database db_hive4;
set mapreduce.framework.name;


//1、文本文件格式 textfile(一般常用orc和parquet)
create table textfile_table(
    id int,
    name string
)
stored as textfile ;

//2、ORC格式(列式存储)

//列式存储,在查询只需要少数几个字段的时候，能大大减少读取的数据量

//2.1 由header、body、tail三部分组成

/*
    header内容为orc，表示文件类型

    Body是由多个stripe组成,每个stripe为hdfs为块大小,每个stripe里有三部分组成，分别是Index Data，Row Data，Stripe Footer。

    Tail由File Footer和PostScript组成
    File Footer中保存了各Stripe的其实位置、索引长度、数据长度等信息，各Column的统计信息等
    PostScript记录了整个文件的压缩类型以及File Footer的长度信息等。
 */

//建表
create table orc_table(
    id int,
    name string
)
stored as orc;

//默认使用serde读写文件
show create table orc_table;

//2、parquest文件基本格式

//由row group和footer组成
//每个row group包含多个column chunck,每个chunck包含多个page

/*
 行组（Row Group）：一个行组对应逻辑表中的若干行。
列块（Column Chunk）：一个行组中的一列保存在一个列块中。
页（Page）：一个列块的数据会划分为若干个页。

 */

//footer存储了每个行组的中每个列狂的元数据信息

//2、1创建表
create table parquet(
    id int,
    name string
)
stored as parquet;
