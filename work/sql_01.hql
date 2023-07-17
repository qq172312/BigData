set mapreduce.framework.name=local;
//作业1
//3.2.2 查询每门课程有多少学生参加了考试（有考试成绩

select course_name
     , count(stu_id) num
from score_info
         join course_info ci on score_info.course_id = ci.course_id
group by course_name;

//3.4.2 按照如下格式显示学生的语文、数学、英语三科成绩，没有成绩的输出为0，按照学生的有效平均成绩降序显示
//学生id 语文 数学 英语 有效课程数 有效平均成绩

select s1.stu_id,
       sum(`if`(ci.course_name = '语文', score, 0)) `语文`,
       sum(`if`(ci.course_name = '数学', score, 0)) `数学`,
       sum(`if`(ci.course_name = '英语', score, 0)) `英语`,
       count(*)                                     `有效课程数`,
       avg(s1.score)                                `有效平均成绩`
from score_info s1
         join course_info ci
              on s1.course_id = ci.course_id
group by s1.stu_id;

//3.4.3 查询一共参加三门课程且其中一门为语文课程的学生的id和姓名
select t1.stu_id, t1.one
from (select s1.stu_id,
             `if`(count(*) over (partition by s1.stu_id ) = 3, `if`(s1.course_id = 1, si.stu_name, null), null) one
      from score_info s1
               join student_info si on s1.stu_id = si.stu_id) t1
where t1.one is not null;

//问题:去除运算之后的null(不在表中的数据不能用where过滤null值)
select s1.stu_id,
       `if`(count(*) over (partition by s1.stu_id ) = 3, `if`(s1.course_id = 1, si.stu_name, 0), 0) one
from score_info s1
         join student_info si on s1.stu_id = si.stu_id;

//3、3 分组结果的条件

set mapreduce.framework.name;
//3.3.1 查询平均成绩大于60分的学生的学号和平均成绩
select stu_id,
       round(avg(score), 2) avg_score
from score_info
group by stu_id
having round(avg(score), 2) > 60 //有时不可使用别名排序
order by avg_score;



desc function round;

//3.3.2 查询至少选修四门课程的学生学号
select t1.stu_id
from (select distinct stu_id,
                      count(course_id) over (partition by stu_id) num
      from score_info) t1
where t1.num >= 4;

//性能更好
select stu_id
from score_info
group by stu_id
having count(course_id) >= 4;

//3.3.3 查询同姓（假设每个学生姓名的第一个字为姓）的学生名单并统计同姓人数大于2的姓
select substr(stu_name, 1, 1)
from student_info
group by substr(stu_name, 1, 1)
having count(*) > 1;

//3.3.4 查询每门课程的平均成绩，结果按平均成绩升序排序，平均成绩相同时，按课程号降序排列
select course_id, avg(score) avg_score
from score_info
group by course_id
order by avg(score), course_id desc;

//3.3.5 统计参加考试人数大于等于15的学科
select course_id, count(*) num
from score_info
group by course_id
having num >= 15;

//3.4 查询结果排序&分组指定条件

//3.4.2 按照如下格式显示学生的语文、数学、英语三科成绩，没有成绩的输出为0，按照学生的有效平均成绩降序显示
select si.stu_id,
       sum(`if`(ci.course_name = '语文', si.score, 0)) `语文`,
       sum(`if`(ci.course_name = '数学', si.score, 0)) `数学`,
       sum(`if`(ci.course_name = '英语', si.score, 0)) `英语`,
       round(avg(si.score), 2)                         `平均成绩`
from score_info si
         join course_info ci on si.course_id = ci.course_id
group by si.stu_id
order by `平均成绩` desc;


//4、复杂查询

//4.1 子查询

//4.1.1 [课堂讲解]查询所有课程成绩均小于60分的学生的学号、姓名

select stu_id, stu_name
from student_info
where stu_id in (select stu_id
                 from score_info
                 group by stu_id
                 having sum(`if`(score >= 60, 1, 0)) = 0);

//性能同上
select s.stu_id,
       s.stu_name
from (select stu_id,
             sum(if(score >= 60, 1, 0)) flag
      from score_info
      group by stu_id
      having flag = 0) t1
         join student_info s on s.stu_id = t1.stu_id;

//5、多表查询(尽量避免子查询)

//5.1 表连接

//5.1.1 [课堂讲解]查询有两门以上的课程不及格的同学的学号及其平均成绩
select stu_id, stu_name
from student_info
where stu_id in
      (select stu_id
       from score_info
       group by stu_id
       having sum(`if`(score < 60, 1, 0)) > 2);

//参考
select t1.stu_id,
       t2.avg_score
from (select stu_id,
             sum(if(score < 60, 1, 0)) flage
      from score_info
      group by stu_id
      having flage >= 2) t1
         join (select stu_id,
                      avg(score) avg_score
               from score_info
               group by stu_id) t2 on t1.stu_id = t2.stu_id;


//5.2.1 课程编号为"01"且课程分数小于60，按分数降序排列的学生信息

select t2.stu_id,
       t2.stu_name,
       t2.birthday,
       t2.sex,
       t1.score
from student_info t2
         join
     (select stu_id,
             score
      from score_info
      where course_id = '01'
        and score < 60) t1
     on t1.stu_id = t2.stu_id
    sort by score;

//5.2.2 查询所有课程成绩在70分以上的学生的姓名、课程名称和分数，按分数升序排列

select t2.stu_name,
       t4.course_name,
       t3.score
from student_info t2
         join
     (select stu_id
      from score_info
      group by stu_id
      having count(*) = count(`if`(score > 70, 1, null))) t1
     on t1.stu_id = t2.stu_id
         join score_info t3 on t3.stu_id = t1.stu_id
         join course_info t4 on t4.course_id = t3.course_id
    sort by t3.score;

//5.2.3 查询该学生不同课程的成绩相同的学生编号、课程编号、学生成绩

select t1.stu_id,
       t1.course_id,
       t2.score
from score_info t1
         join score_info t2
              on t1.score = t2.score
                  and t1.course_id <> t2.course_id
                  and t1.stu_id = t2.stu_id
    sort by t1.stu_id;

//

//5.2.6 [课堂讲解]查询学过“李体音”老师所教的所有课的同学的学号、姓名

//解法一,效率低
select stu_id, stu_name
from student_info
where stu_id in (select t4.stu_id
                 from teacher_info t1
                          join course_info t2 on t1.tea_id = t2.tea_id
                          join score_info t3 on t3.course_id = t2.course_id
                          join student_info t4 on t4.stu_id = t3.stu_id
                 where t1.tea_name = '李体音'
                 group by t4.stu_id
                 having count(t2.course_id) = 2);
//group by 之后在select中不能单一使用组内字段,只能配合函数

//解法2效率高 6s
//可以将group by之后的多行使用collect_set去重,然后取出第一个元素
select t4.stu_id,
       collect_set(t4.stu_name)[0] `stu_name`
from teacher_info t1
         join course_info t2 on t1.tea_id = t2.tea_id
         join score_info t3 on t3.course_id = t2.course_id
         join student_info t4 on t4.stu_id = t3.stu_id
where t1.tea_name = '李体音'
group by t4.stu_id
having count(t2.course_id) = 2;

//效率同上 6s
select t4.stu_id,
       collect_set(t4.stu_name)[0]
from (select tea_id
      from teacher_info
      where tea_name = '李体音') t1
         join course_info t2
              on t1.tea_id = t2.tea_id
         join score_info t3
              on t2.course_id = t3.course_id
         join student_info t4
              on t4.stu_id = t3.stu_id
group by t4.stu_id
having count(t2.course_id) = 2;

//5.2.7  [课堂讲解]查询学过“李体音”老师所讲授的任意一门课程的学生的学号、姓名
select t4.stu_id,
       t4.stu_name
from teacher_info t1
         join course_info t2 on t1.tea_id = t2.tea_id
         join score_info t3 on t3.course_id = t2.course_id
         join student_info t4 on t4.stu_id = t3.stu_id
where t1.tea_name = '李体音'
group by t4.stu_id,t4.stu_name;

//5.2.8 [课堂讲解]查询没学过"李体音"老师讲授的任一门课程的学生姓名
//collect_set性能和group by分组一样
select t4.stu_id,
       t4.stu_name stu_name
from teacher_info t1
         join course_info t2 on t1.tea_id = t2.tea_id and t1.tea_name = '李体音'
         right join score_info t3 on t2.course_id = t3.course_id
         right join student_info t4 on t4.stu_id = t3.stu_id
group by t4.stu_id,t4.stu_name
having count(tea_name) = 0;

//5.2.9 [课堂讲解]查询至少有一门课与学号为“001”的学生所学课程相同的学生的学号
//在这里distinct和group by性能差不多
select
    t1.stu_id
from (
select stu_id,
       row_number() over (partition by stu_id) num
from score_info
where course_id in (select course_id
                    from score_info
                    where stu_id = 1)
  and stu_id <> 1) t1
where t1.num=1;

show tables in db_hive3 like '*order*';



