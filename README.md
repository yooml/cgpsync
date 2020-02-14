# cgpsync

#### 1.开发背景
cgpsync是一个同步greenplum数据库中的表到另一个gp集群中的工具。

greenplum没有针对整个集群的备份功能，在某些场景有从一个gp集群备份到另一个gp集群的需求。

cgpsync主要解决备份过程中单个表数据同步的问题，比如：有个大表数据量特别大，单对单同步的时间长。这个场景可以将其按时间分区然后使用cgpsync进行同步：

cgpsync可以以分区子表为单位，一个或多个子表的并行同步，并且已同步过的表会有时间记录确保下次不重复同步。


#### 2.使用说明
没有golang环境可以直接用可执行文件，这里以有golang环境为例，查看使用帮助：
```
 ./cgpsync -h
//或者：
go run cgpsync.go -h
```
打印如下：
```
  -e string
        同步截止时间
  -g int
        使用的goruntine（并发）数量，默认3 (default 3)
  -h    查看帮助
  -t string
        同步表名
```

-e 一般为当前时间，用于程序判断需要同步哪些子表
##### -e的使用场景描述：
1.如果一个分区表以年为单位分区，则-e 2017*开头(如-e 20170101)的参数含义为：
同步dbname中-t指定的某个表的2017年的分区以及2017年以前分区的数据到destination_dbname的对应表中。

2.如果之前对同一张表执行过-e 20170101，则再执行-e 20170101时，cgpsync的实际操作是：
只同步此表的2017年的分区到对应表中（因为通过查询sync_table和v_gp_range_partition_meta得知2017年之前的分区之前已经同步过了）

#### 3.配置要求

cgpsync需要放在目标机器(destination_host)并且在root或者gp所在账号(如:gpadmin)下使用，配置文件名config.json，需要放在程序同一目录下，参数如下
```
{
  "host": "yourhost",
  "user": "gpadmin",
  "port": 2345,
  "password": "yourpassword",
  "dbname": "gpdb",
  "destination_host": "yourhost",
  "destination_user": "gpadmin",
  "destination_port": 2345,
  "destination_password": "yourpassword",
  "destination_dbname":"testdb"
}
```
‘destination_’开头的是目的数据库的连接配置，非‘destination_’的是源数据库。

#### 4.使用用例
完整用例如下：
```
 go run cgpsync.go -e 20170201 -t persons -g 5
 ```

同步截止时间为20170201，同步的表名为persons，使用5个goruntine同时在跑。

使用可执行文件类似：
```
./cgpsync -e 20170201 -t persons
```

#### 5.注意事项
1.cgpsync有使用到视图v_gp_range_partition_meta，创建此视图的sql如下：
```sql
create or replace view v_gp_range_partition_meta as  SELECT pp.parrelid::regclass table_name,pr1.parchildrelid::regclass child_tbl_name,pr1.parname as partition_name,pr1.parruleord as partitionposition,translate(pg_get_expr(pr1.parrangestart,pr1.parchildrelid),'-'':date character varying bpchar numeric double percision timestamp without time zone','') as partitionrangestart,translate(pg_get_expr(pr1.parrangeend,pr1.parchildrelid),'-'':date character varying bpchar numeric double percision timestamp without time zone','') as partitionrangeend,substring(parrangeend,'consttype ([0-9]+)')::integer::regtype rangetype FROM  pg_partition pp, pg_partition_rule pr1 where pp.paristemplate = false and pr1.paroid=pp.oid and pp.parkind = 'r';
```
还有检查是否之前同步过的的表：
```sql
create table sync_table
(
table_name varchar(64),
end_tm varchar(64)
);
```
以上两条sql都需要在destination_dbname所在的gp库中执行，且此程序暂时只能在destination_host下执行。

2.同步的目标子表会被truncate清空以确保同步数据的一致。

#### 6.修改原理

视图查询sql由原先的
```sql
 select table_name,child_tbl_name, partitionrangeend from v_gp_range_partition_meta where table_name='test_partition_range'::regclass  and partitionrangeend >'20170131' and partitionrangeend <'20170201' order by partitionrangeend;
```
如上如果是以月为分区的子表，原先的sql在endtime输入为201702**的情况下并不能够查到2017年2月份的子表信息，也就不会同步2月份的子表。

修改为：
```sql
 select table_name,child_tbl_name, partitionrangeend from v_gp_range_partition_meta where table_name='test_partition_range'::regclass  and partitionrangeend >'20170131' and partitionrangestart <'20170201(64)' order by partitionrangeend;
```
在endtime设置为201702**时可以查询到2017年2月份的子表信息，然后同步。
