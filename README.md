# cgpsync

#### 1.开发背景
cgpsync是一个同步greenplum数据库子表的工具。

想要实现一个主表下多个子表同时同步的功能，并且解决不同子表同步完成后阻塞等待的问题。


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

#### 3.配置要求

cgpsync需要放在目标机器并且在root账号下使用，配置文件名config.json，需要放在程序同一目录下，参数如下
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
