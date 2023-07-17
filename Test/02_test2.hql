
show create table dept;
show databases ;
use db_hive1;
show tables ;
desc formatted test_stu1;
use default;
select * from test_stu1;
//改表名
alter table test_stu rename to test_stu1;

//增加列
alter table test_stu1 add columns (sex string,age int);

//更改列名
alter table test_stu1 change column friends friend array<string>;
select * from test_stu1;

//更改列数据类型
alter table test_stu1 change column friend friend array<int>;
desc formatted test_stu1;

//调整列位置 first after//报错，数据类型统一往下走(元数据和hdfs分离按位置对应)
alter table test_stu1 change sex sex string first;


//2.3 insert overwrite 只导入数据
insert overwrite table student1
select * from student1;

/*
 INSERT OVERWRITE [LOCAL] DIRECTORY directory
  [ROW FORMAT row_format] [STORED AS file_format] select_statement;

 */

//2.3.1 导入数据(local)数据到本地
insert overwrite local directory '本地文件'
select * from student1;

//2.3.2 导入数据(无local)数据到hdfs
insert overwrite directory '本地文件'
select * from student1;

//2.3.3 导入数据(导入指定格式)
insert overwrite directory '本地文件'
row format delimited fields terminated by '-'
select * from student1;

//4. export&import(完全导入/导出，表的结构和数据)

//4.1 export(会导出表的元数据)
/*
 EXPORT TABLE tablename TO 'export_target_path'
 */
export table student1 to '/user/hive/warehouse/export/student1_ex';

//4.2 import
/*
 IMPORT [EXTERNAL] TABLE new_or_original_tablename FROM 'source_path' [LOCATION 'import_target_path']
 */
import table student2 from '/user/hive/warehouse/export/student';