package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"os/exec"
	"time"
	"github.com/spf13/viper"
)

var (
	h bool

	//v bool
	parallel_cnt int
	pyfile string
	table_name string
	sync_end_time string
)

func init()  {
	flag.BoolVar(&h, "h", false, "查看帮助")

	//flag.BoolVar(&v, "v", false, "show version and exit")
	flag.IntVar(&parallel_cnt,"g",3,"使用的goruntine（并发）数量，默认3")
	//flag.StringVar(&pyfile, "py", "", "所使用的外部python文件名（放在同一路径下）")
	flag.StringVar(&sync_end_time, "e", time.Now().Format("20060102"), "同步截止时间")
	flag.StringVar(&table_name, "t", "", "同步表名")

}

func usage() {
	//fmt.Fprintf(os.Stderr, `cgpsync version: 1.0.0 Options: `)
	flag.PrintDefaults()
}

type V_gp_range_partition_meta struct {
	table_name string
	child_tbl_name string
	partition_name string
	partitionposition string
	partitionrangestart string
	partitionrangeend string
	rangetype string
}

type Sync_type struct {
	table_name string
	end_tm string
}

func config_parse() *viper.Viper {
	dbConfig := viper.New()
	dbConfig.SetConfigName("config.json")     // name of config file (without extension)
	dbConfig.AddConfigPath("./") // optionally look for config in the working directory
	dbConfig.SetConfigType("json")
	err := dbConfig.ReadInConfig() // Find and read the config file
	if err != nil {                  // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	return dbConfig
}

func main()  {
	flag.Parse()
	if h{
		flag.Usage()
		os.Exit(1)
	}

	//读取配置文件
	dbConfig:=config_parse()

	db := Connect(dbConfig)
	sql_do_select_sync_table:=fmt.Sprintf(`select table_name, end_tm from sync_table where table_name='%s'`,table_name)
	start_time:=do_select_sync_table(db,sql_do_select_sync_table)
	if start_time>sync_end_time{
		log.Println("end_time值",sync_end_time,"早于原先同步过的时间：",start_time)
		os.Exit(1)
	}
	sql_v:=fmt.Sprintf(`select table_name,child_tbl_name, partitionrangeend from v_gp_range_partition_meta where table_name='%s'::regclass  and partitionrangeend >'%s' and partitionrangestart <= '%v(64)' order by partitionrangeend`,table_name,start_time,sync_end_time)
	v_gp_range_partition_metas:=do_select_v_gp_range_partition_meta(db,sql_v)
	all_sql_v:=fmt.Sprintf(`select table_name,child_tbl_name, partitionrangeend from v_gp_range_partition_meta where table_name='%s'::regclass order by partitionrangeend`,table_name)
	all_v_gp_range_partition_metas:=do_select_v_gp_range_partition_meta(db,all_sql_v)

	ch2:=make(chan string,30)
	ch3:=make(chan *V_gp_range_partition_meta,len(v_gp_range_partition_metas))

	for n:=0;n<len(v_gp_range_partition_metas);n++{
		ch3<-&v_gp_range_partition_metas[n]
	}

	//创建工作目录
	mkdir:=fmt.Sprintf("mkdir /tmp/cgpsync")
	makedir:=exec.Command("/bin/bash", "-c",mkdir)
	if err := makedir.Run(); err != nil {
		fmt.Println("Error: ", err, "|", makedir.Stderr)
	}

	//非分区表同步
	if len(v_gp_range_partition_metas)==0 && len(all_v_gp_range_partition_metas)==0{
		chown:=fmt.Sprintf("mkfifo /tmp/cgpsync/%s.pipe",table_name)
		makepipe:=exec.Command("/bin/bash","-c",chown)
		stderr := &bytes.Buffer{}
		makepipe.Stderr=stderr
		if err := makepipe.Run(); err != nil {
			fmt.Println("Error: ", err, "|", stderr.String())
		}
		ch4:=make(chan string,1)
		sql_copyfrom:=fmt.Sprintf("copy %s from '/tmp/cgpsync/%s.pipe';",table_name,table_name)
		go func() {
			log.Println("开始同步表：",table_name)
			copy_from(sql_copyfrom,table_name,ch4,table_name,db)
		}()
		echo:=fmt.Sprintf("psql -h %s -p %d -U %s -d %s -c 'copy %s to stdout' > /tmp/cgpsync/%s.pipe",
			dbConfig.GetString("host"), dbConfig.GetInt("port"), dbConfig.GetString("user"), dbConfig.GetString("dbname"),table_name,table_name)
		py :=exec.Command("/bin/bash","-c",echo)
		stderr = &bytes.Buffer{}
		py.Stderr = stderr
		if err := py.Run(); err != nil {
			fmt.Println("Error: ", err, "|", stderr.String())
		}
	}else {
		log.Println("len(v_gp_range_partition_metas)准备同步的子表总数：",len(v_gp_range_partition_metas))
	}

	if pyfile==""{
		for m:=0;m<parallel_cnt;m++ {
			chown:=fmt.Sprintf("mkfifo /tmp/cgpsync/%d.pipe",m)
			makepipe:=exec.Command("/bin/bash","-c",chown)
			stderr := &bytes.Buffer{}
			makepipe.Stderr=stderr
			if err := makepipe.Run(); err != nil {
				fmt.Println("Error: ", err, "|", stderr.String())
			}
			go func(m int) {
				go_sync_one_part_name(ch3,ch2,m,db,dbConfig)
			}(m)
		}
		//flag.Usage()
		//os.Exit(1)
	}else {
		log.Println("执行python文件:"+pyfile)
		for m:=0;m<parallel_cnt;m++ {
			go func(m int) {
				py_sync_one_part_name(ch3,ch2,m)
			}(m)
		}
	}

	// 下面这个for循环的意义就是利用信道的阻塞，一直从信道里取数据，直到取得跟并发数一样的个数的数据，则视为所有goroutines完成。
	for i:=0;i<len(v_gp_range_partition_metas);i++{
		<- ch2
	}

	//操作完后更新sync_table表中数据
	sql_do_select_sync_table=fmt.Sprintf(`select table_name, end_tm from sync_table where table_name='%s'`,table_name)
	start_time=do_select_sync_table(db,sql_do_select_sync_table)
	if start_time==""{
		sql_update_sync_table := fmt.Sprintf("insert into sync_table values('%s', '%s');",table_name,sync_end_time)
		_,err:=db.Exec(sql_update_sync_table)
		if err != nil {
			panic(err)
		}
	}else {
		if start_time<sync_end_time{
			sql_update_sync_table := fmt.Sprintf("update sync_table set end_tm='%s' WHERE table_name='%s'",sync_end_time,table_name)
			_,err:=db.Exec(sql_update_sync_table)
			if err != nil {
				panic(err)
			}
		}
	}
	//删除工作目录
	delete_work_dir:=exec.Command("/bin/bash","-c","rm -rf /tmp/cgpsync")
	stderr := &bytes.Buffer{}
	delete_work_dir.Stderr=stderr
	if err := delete_work_dir.Run(); err != nil {
		fmt.Println("Error: ", err, "|", stderr.String())
	}
}

func do_select_sync_table(db *sql.DB,sql string) string {
	rows,err:=db.Query(sql)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var sync_types []Sync_type
	var sync_type Sync_type
	for rows.Next(){
		rows.Scan(&sync_type.table_name,&sync_type.end_tm)
		sync_types = append(sync_types, sync_type)
	}
	if len(sync_types)==0{
		return ""
	}
	return sync_types[0].end_tm
}

func do_select_v_gp_range_partition_meta(db *sql.DB,sql string) []V_gp_range_partition_meta{
	rows,err:=db.Query(sql)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var v_gp_range_partition_metas []V_gp_range_partition_meta
	for rows.Next(){
		var v_gp_range_partition_meta V_gp_range_partition_meta
		rows.Scan(&v_gp_range_partition_meta.table_name,&v_gp_range_partition_meta.child_tbl_name,&v_gp_range_partition_meta.partitionrangeend)
		v_gp_range_partition_metas = append(v_gp_range_partition_metas, v_gp_range_partition_meta)
		//如果查询出来子表(child_tbl_name)为空，下面运行的子进程就会卡住，如要修改可以这里做个判断或者下面子进程做判断
	}
	return v_gp_range_partition_metas
}


func go_sync_one_part_name(ch3 <-chan *V_gp_range_partition_meta,ch2 chan<- string,m int,db *sql.DB,dbconfig *viper.Viper)  {
	for i :=range ch3{
		ch4:=make(chan string,1)
		sql_copyfrom:=fmt.Sprintf("copy %s from '/tmp/cgpsync/%d.pipe';",i.child_tbl_name,m)

		go func() {
			log.Println("开始同步表：",i.table_name,".",i.child_tbl_name)
			copy_from(sql_copyfrom,i.child_tbl_name,ch4,i.table_name,db)
		}()

		echo:=fmt.Sprintf("psql -h %s -p %d -U %s -d %s -c 'copy %s to stdout' > /tmp/cgpsync/%d.pipe",
			dbconfig.GetString("host"), dbconfig.GetInt("port"), dbconfig.GetString("user"), dbconfig.GetString("dbname"),i.child_tbl_name,m)
		py :=exec.Command("/bin/bash","-c",echo)
		stderr := &bytes.Buffer{}
		py.Stderr = stderr
		if err := py.Run(); err != nil {
			fmt.Println("Error: ", err, "|", stderr.String())
		}
		<-ch4
		ch2<-i.child_tbl_name
	}
}

func copy_from(sql_copyfrom string,child_tbl_name string,ch4 chan string,table_name string,db *sql.DB)  {
	sql_truncate_table:=fmt.Sprintf("truncate table %s",child_tbl_name)
	_,err:=db.Exec(sql_truncate_table)
	if err != nil {
		panic(err)
	}
	_,err=db.Exec(sql_copyfrom)
	if err != nil {
		panic(err)
	}else {
		ch4<- table_name
		if child_tbl_name==table_name{
			log.Println("同步完成：",table_name)
		}else {
			log.Println("同步完成：",table_name,".",child_tbl_name)
		}
	}
}


func py_sync_one_part_name(ch3 <-chan *V_gp_range_partition_meta,ch2 chan<- string,m int)  {
	for i :=range ch3{
	time.Sleep(time.Second)
	dom:=fmt.Sprintf("%d",m)
	logout:=fmt.Sprintf("开始同步表：%s，所用管道文件：%s.pipe",i.child_tbl_name,dom)
	log.Println(logout)
	py :=exec.Command("python",pyfile,dom,i.child_tbl_name,i.partitionrangeend)
	stderr := &bytes.Buffer{}
	stdout := &bytes.Buffer{}
	py.Stderr = stderr
	py.Stdout = stdout
	if err := py.Run(); err != nil {
		fmt.Println("Error: ", err, "|", stderr.String())
	} else {
		ch2<-i.child_tbl_name
		fmt.Println(stdout.String())
	}
	}
}

func dopy(ch chan<- int,ch1 <-chan int,parallel_cnt int,table_name string,v_gp_range_partition_metas []V_gp_range_partition_meta)  {
	var test []byte
	for _,v := range v_gp_range_partition_metas{

		test=append(test,byte(1))
		doo:=fmt.Sprintf("%v",parallel_cnt)
		py :=exec.Command("python3","do3.py",doo,v.child_tbl_name)
		stderr := &bytes.Buffer{}
		stdout := &bytes.Buffer{}
		py.Stderr = stderr
		py.Stdout = stdout
		if err := py.Run(); err != nil {
			fmt.Println("Error: ", err, "|", stderr.String())
		} else {
			buf := bytes.NewBuffer(test)
			var i2 int
			binary.Read(buf, binary.BigEndian, &i2)
			fmt.Println(i2)     // 511
			ch<-i2
			fmt.Println(stdout.String())
		}
	}
}


func Connect(dbconfig *viper.Viper)(*sql.DB){
	//sslmode=verify-full  sslmode=disable
	/*psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		dbconfig.GetString("host"), dbconfig.GetInt("port"), dbconfig.GetString("user"), dbconfig.GetString("password"), dbconfig.GetString("dbname"))*/
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		dbconfig.GetString("destination_host"), dbconfig.GetInt("destination_port"), dbconfig.GetString("destination_user"), dbconfig.GetString("destination_password"), dbconfig.GetString("destination_dbname"))
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err)
	}
	fmt.Println("Successfully connected!")
	return db
}
