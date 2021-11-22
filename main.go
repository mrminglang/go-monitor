package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Reader 抽象读取接口
type Reader interface {
	Read(rc chan []byte)
}

// Writer 抽象写入接口
type Writer interface {
	Write(wc chan *Message)
}

// ReadFromFile 读取模块
type ReadFromFile struct {
	path string // 读取文件路径
}

// WriteToInfluxDB 写入模块
type WriteToInfluxDB struct {
	influxDBDsn string // influx data source
}

// LogProcess 日志进程
type LogProcess struct {
	rc    chan []byte
	wc    chan *Message
	read  Reader
	write Writer
}

// Message 日志内容
type Message struct {
	TimeLocal                    time.Time
	BytesSent                    int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime    float64
}

// SystemInfo 系统状态监控
type SystemInfo struct {
	HandleLine   int     `json:"handle_line"`    // 总处理日志行数
	Tps          float64 `json:"tps"`            // 系统吞吐量
	ReadChanLen  int     `json:"read_chan_len"`  // read channel 长度
	WriteChanLen int     `json:"write_chan_len"` // write channel 长度
	RunTime      string  `json:"run_time"`       // 运行总时间
	ErrNum       int     `json:"err_num"`        // 错误数
}

const (
	TypeHandleLine = 0
	TypeErrNum     = 1
)

var TypeMonitorChan = make(chan int, 200)

type Monitor struct {
	startTime time.Time
	data      SystemInfo
	tpsSli    []int
}

func (m *Monitor) start(lp *LogProcess) {
	// 消费数据
	go func() {
		for n := range TypeMonitorChan {
			switch n {
			case TypeErrNum:
				m.data.ErrNum += 1
			case TypeHandleLine:
				m.data.HandleLine += 1
			}
		}
	}()

	// 吞吐量
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			<-ticker.C
			m.tpsSli = append(m.tpsSli, m.data.HandleLine)
			if len(m.tpsSli) > 2 {
				m.tpsSli = m.tpsSli[1:]
			}
		}
	}()

	// http
	http.HandleFunc("/monitor", func(writer http.ResponseWriter, request *http.Request) {
		m.data.RunTime = time.Now().Sub(m.startTime).String()
		m.data.ReadChanLen = len(lp.rc)
		m.data.WriteChanLen = len(lp.wc)
		if len(m.tpsSli) >= 2 {
			m.data.Tps = float64(m.tpsSli[1] - m.tpsSli[0]) / 5
		}

		ret, _ := json.MarshalIndent(m.data, "", "\t")
		io.WriteString(writer, string(ret))
	})
	http.ListenAndServe(":9193", nil)
}

// 读取方法
func (r *ReadFromFile) Read(rc chan []byte) {
	// 打开文件
	f, err := os.Open(r.path)
	if err != nil {
		panic(fmt.Sprintf("open file error:%s", err.Error()))
	}

	// 从文件末尾开始逐行读取
	f.Seek(0, 2) // 字符指针移到最后
	rd := bufio.NewReader(f)

	// 循环读取文件
	for {
		line, err := rd.ReadBytes('\n')
		if err == io.EOF { // 读取带文件末尾
			time.Sleep(500 * time.Millisecond) // 500毫秒
			continue
		} else if err != nil {
			panic(fmt.Sprintf("ReadBytes error:%s", err.Error()))
		}
		// 写入read channel
		TypeMonitorChan <- TypeHandleLine
		rc <- line[:len(line)-1] // 去掉末尾换行符
	}
}

// Process 解析模块
func (l *LogProcess) Process() {
	/**
	  172.18.0.1 - - [17/Nov/2021:03:22:06 +0000] http "POST /API/automateApi/downloadFile HTTP/1.1" 200 149 "-" "downloadFile"  "-" 1.005 1.854
	*/
	localTime, _ := time.LoadLocation("Asia/Shanghai")

	// 正则匹配
	reg := regexp.MustCompile(`([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)

	// 循环解析
	for value := range l.rc {
		ret := reg.FindStringSubmatch(string(value))
		if len(ret) != 14 {
			TypeMonitorChan <- TypeErrNum
			log.Println("FindStringSubmatch fail:", string(value))
			continue
		}

		message := &Message{}
		// 时间处理
		timeLocal, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000", ret[4], localTime)
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Println("ParseInLocation fail:", err.Error(), ret[4])
			continue
		}
		message.TimeLocal = timeLocal

		byteSent, _ := strconv.Atoi(ret[8])
		message.BytesSent = byteSent

		// POST /API/automateApi/downloadFile HTTP/1.1
		reqSplit := strings.Split(ret[6], " ") //按照空格进行切割
		if len(reqSplit) != 3 {
			TypeMonitorChan <- TypeErrNum
			log.Println("Strings Split fail:", ret[6])
			continue
		}
		message.Method = reqSplit[0]

		u, err := url.Parse(reqSplit[1])
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Println("url parse fail:", err)
			continue
		}
		message.Path = u.Path
		message.Scheme = ret[5]
		message.Status = ret[7]
		upstreamTime, _ := strconv.ParseFloat(ret[12], 64)
		requestTime, _ := strconv.ParseFloat(ret[13], 64)
		message.UpstreamTime = upstreamTime
		message.RequestTime = requestTime

		l.wc <- message
	}
}

// 写入方法
func (w *WriteToInfluxDB) Write(wc chan *Message) {
	influxDBDsn := strings.Split(w.influxDBDsn, "@")

	// influxdb客户端：https://github.com/influxdata/influxdb/tree/1.7/client
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     influxDBDsn[0],
		Username: influxDBDsn[1],
		Password: influxDBDsn[2],
	})
	if err != nil {
		log.Fatal(err)
	}

	defer c.Close()

	for value := range wc {
		// Create a new point batch
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  influxDBDsn[3],
			Precision: influxDBDsn[4],
		})
		if err != nil {
			panic(fmt.Sprintf("client new batch points fail", err))
		}

		// Create a point and add to batch
		// tags : path、method、scheme、status
		tags := map[string]string{
			"Path":   value.Path,
			"Method": value.Method,
			"Scheme": value.Scheme,
			"Status": value.Status,
		}

		// fields：UpstreamTime、RequestTime、BytesSent
		fields := map[string]interface{}{
			"UpstreamTime": value.UpstreamTime,
			"RequestTime":  value.RequestTime,
			"BytesSent":    value.BytesSent,
		}

		// name:tableName
		pt, err := client.NewPoint("nginx_log", tags, fields, value.TimeLocal)
		if err != nil {
			panic(fmt.Sprintf("client new point fail", err))
		}
		bp.AddPoint(pt)

		// Write the batch
		if err := c.Write(bp); err != nil {
			panic(fmt.Sprintf("client write fail", err))
		}

		// Close client resources
		if err := c.Close(); err != nil {
			panic(fmt.Sprintf("client close fail", err))
		}

		fmt.Println("write success!")
	}
}

func main() {
	// 参数注入方式:go run main.go -path ./storage/logs/db-2021-09-16.log -influxDBDsn http://127.0.0.1:8086@ming@123456@monitor@s
	var path, influxDBDsn string
	flag.StringVar(&path, "path", "./storage/logs/db-2021-09-16.log", "read file data")
	flag.StringVar(&influxDBDsn, "influxDBDsn", "http://localhost:8086@root@12345678@monitor@s", "influx data source")
	flag.Parse()

	r := &ReadFromFile{
		path: path,
	}
	w := &WriteToInfluxDB{
		influxDBDsn: influxDBDsn,
	}
	lp := &LogProcess{
		rc:    make(chan []byte, 200),
		wc:    make(chan *Message, 200),
		read:  r,
		write: w,
	}

	go lp.read.Read(lp.rc)
	go lp.Process() // 可以多开几个协程
	go lp.write.Write(lp.wc) // 可以多开几个协程

	// 运行监控模块
	m := &Monitor{
		startTime: time.Now(),
		data:      SystemInfo{},
	}
	m.start(lp)
}
