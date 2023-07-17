//optimize

explain select 1-1 ;

//创建表
//2000w
create table order_detail(
                             id           string comment '订单id',
                             user_id      string comment '用户id',
                             product_id   string comment '商品id',
                             province_id  string comment '省份id',
                             create_time  string comment '下单时间',
                             product_num  int comment '商品件数',
                             total_amount decimal(16, 2) comment '下单金额'
)
    partitioned by (dt string)
    row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive-3.1.3/datas/optimize/order_detail.txt'
overwrite into table order_detail partition (dt='2020-06-14');

//600w
create table payment_detail(
                               id              string comment '支付id',
                               order_detail_id string comment '订单明细id',
                               user_id         string comment '用户id',
                               payment_time    string comment '支付时间',
                               total_amount    decimal(16, 2) comment '支付金额'
)
    partitioned by (dt string)
    row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive-3.1.3/datas/optimize/payment_detail.txt'
    overwrite into table payment_detail partition (dt='2020-06-14');

//100w
create table product_info(
                             id           string comment '商品id',
                             product_name string comment '商品名称',
                             price        decimal(16, 2) comment '价格',
                             category_id  string comment '分类id'
)
    row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive-3.1.3/datas/optimize/product_info.txt'
    overwrite into table product_info;


create table province_info(
                              id            string comment '省份id',
                              province_name string comment '省份名称'
)
    row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive-3.1.3/datas/optimize/province_info.txt'
    overwrite into table province_info;

//优化测试
set mapreduce.framework.name;

//map
set mapreduce.map.memory.mb;
set mapreduce.map.cpu.vcores=1;
//reduce
set mapreduce.reduce.memory.mb;
set mapreduce.reduce.cpu.vcores=1;

select course_id,count(*) num
from default.score_info
group by course_id
having num>=15;



