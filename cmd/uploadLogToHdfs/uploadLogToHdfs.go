package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const HADOOP_BIN = "/usr/bin/hadoop"

var HdfsBaseDir = "/ods/mobile/appNewlogs"
var IpList = []string{"172.20.0.91", "172.20.0.93", "172.20.0.106", "172.20.0.52"}

var projectList []string

type Work struct {
	Year           string
	Date           string
	Hour           string
	IsUpload       string
	SrcBaseDir     string
	Project        string
	CombineChan    chan string
	wait           sync.WaitGroup
	CombineWorkNum int
	ZipWorkNum     int
	ProjectNum     int
}

var work = new(Work)
var rWait = sync.WaitGroup{}
var isMajor = "no"

func main() {
	Init()
	if work.IsUpload == "no" {
		infoLog(fmt.Sprintf("# rsync job end date :%s ;hour:%s; minute:%d", work.Date, work.Hour, time.Now().Minute()))
		return
	}

	if len(projectList) > 0 {
		work.wait.Add(1)
		go work.CombineHourFile()

		work.wait.Add(1)
		go work.ZipCombineFile()
		work.wait.Wait()
	}
	infoLog(fmt.Sprintf("###### end job date :%s ;hour:%s", work.Date, work.Hour))
}
func Init() {
	flag.StringVar(&work.Date, "d", GetRunDate("2006-01-02"), "日期 默认为运行时间前一小时的日期")
	flag.StringVar(&work.Hour, "h", GetRunDate("15"), "小时 默认为运行时的前一小时")
	flag.StringVar(&work.Project, "p", "all", "默认全部项目")
	flag.StringVar(&work.IsUpload, "u", GetUploadState(), "是否执行上传")
	flag.StringVar(&isMajor, "m", "yes", "只执行主要任务")

	flag.Parse()

	infoLog(fmt.Sprintf("###### start job date :%s ;hour:%s", work.Date, work.Hour))

	work.Year = strconv.Itoa(GetYearByDate(work.Date))
	work.SrcBaseDir = fmt.Sprintf("/opt/data/appNewLogs/%s/%s/", work.Year, work.Date)
	work.CombineChan = make(chan string, 100)
	work.CombineWorkNum = 10
	work.ZipWorkNum = 12

	for _, ip := range IpList {
		rWait.Add(1)
		go rsync(ip)
	}
	rWait.Wait()

	projectList = GetProjectList(work.SrcBaseDir)
	projectCount := 0
	for _, projectName := range projectList {
		pattern := fmt.Sprintf("*,%s,%s,%s*.log", projectName, work.Date, work.Hour)
		fileList := GetAllFileByPattern(work.SrcBaseDir+projectName, pattern)
		if len(fileList) > 0 {
			projectCount += 1
		}
	}
	work.ProjectNum = projectCount
}

func rsync(ip string) {
	defer rWait.Done()

	dstDir := "/opt/data/appNewLogs/" + work.Year + "/" + work.Date
	Mkdir(dstDir)
	cmd := "/opt/app/rsync/bin/rsync " +
		"-avzP " +
		"--timeout=180 " +
		"--port=3334 " +
		"--bwlimit=512000 " +
		"--include='*/*" + work.Date + ",*' " +
		"--exclude='*/*' " +
		"--password-file=/opt/case/output/qs139166/pass/rsync.pas " +
		"hadoop@" + ip + "::appnewlog/logs/" + work.Year + "/" + work.Date + "/ " +
		dstDir

	if work.IsUpload == "yes" {
		cmd = "/opt/app/rsync/bin/rsync " +
			"-avzP " +
			"--timeout=180 " +
			"--port=3334 " +
			"--bwlimit=512000 " +
			"--include='*/*" + work.Date + "," + work.Hour + "_*'  " +
			"--exclude='*/*' " +
			"--password-file=/opt/case/output/qs139166/pass/rsync.pas " +
			"hadoop@" + ip + "::appnewlog/logs/" + work.Year + "/" + work.Date + "/ " +
			dstDir
	}
	if work.Project != "all" {
		cmd = "/opt/app/rsync/bin/rsync " +
			"-avzP " +
			"--timeout=180 " +
			"--port=3334 " +
			"--bwlimit=512000 " +
			"--include='*/*" + work.Date + ",*' " +
			"--exclude='*/*' " +
			"--password-file=/opt/case/output/qs139166/pass/rsync.pas " +
			"hadoop@" + ip + "::appnewlog/logs/" + work.Year + "/" + work.Date + "/" + work.Project + " " +
			dstDir

		if work.IsUpload == "yes" {
			cmd = "/opt/app/rsync/bin/rsync " +
				"-avzP " +
				"--timeout=180 " +
				"--port=3334 " +
				"--bwlimit=512000 " +
				"--include='*/*" + work.Date + "," + work.Hour + "_*'  " +
				"--exclude='*/*' " +
				"--password-file=/opt/case/output/qs139166/pass/rsync.pas " +
				"hadoop@" + ip + "::appnewlog/logs/" + work.Year + "/" + work.Date + "/" + work.Project + " " +
				dstDir
		}
	}
	Cmd(cmd)
	infoLog("rsync end :" + cmd)
}

//合并各个项目单个小时的文件
func (work *Work) CombineHourFile() {
	defer work.wait.Done()
	pNum := make(chan int, work.CombineWorkNum)
	projectCount := len(projectList)
	results := make(chan bool, projectCount)

	for _, projectName := range projectList {
		pNum <- 1
		go func(project string, rs chan<- bool) {
			pattern := fmt.Sprintf("*,%s,%s,%s*.log", project, work.Date, work.Hour)
			fileList := GetAllFileByPattern(work.SrcBaseDir+project, pattern)
			if len(fileList) > 0 {
				combineFile := fmt.Sprintf("%s%s/%s_%s_%s.txt", work.SrcBaseDir, project, project, work.Date, work.Hour)
				cmd := fmt.Sprintf("cat %s%s/*,%s,%s,%s*.log > %s ", work.SrcBaseDir, project, project, work.Date, work.Hour, combineFile)
				err := Cmd(cmd)
				if err == nil {
					infoLog("combine end : " + project)
					work.CombineChan <- project
				} else {
					errorLog("combine error:" + cmd)
				}
			}

			<-pNum
			rs <- true
		}(projectName, results)
	}

	for i := 0; i < projectCount; i++ {
		<-results
	}
	close(work.CombineChan)
	close(pNum)

}

//压缩合并之后的文件,并上传hdfs
func (work *Work) ZipCombineFile() {
	defer work.wait.Done()
	pNum := make(chan int, work.ZipWorkNum)
	results := make(chan bool, work.ProjectNum)

	for {
		projectName, ok := <-work.CombineChan
		if !ok {
			break
		}
		pNum <- 1
		go func(project string, rs chan<- bool) {
			fileName := fmt.Sprintf("%s%s/%s_%s_%s", work.SrcBaseDir, project, project, work.Date, work.Hour)
			zipCmd := fmt.Sprintf("bzip2 -5 -f %s.txt", fileName)
			//大于500M的文件开启多线程压缩
			fileSize := FileSize(fileName + ".txt")
			if fileSize > 0.5*1024*1024*1024 {
				zipCmd = fmt.Sprintf("pbzip2 -5 -p5f %s.txt", fileName)
			}
			if fileSize > 3*1024*1024*1024 {
				zipCmd = fmt.Sprintf("pbzip2 -5 -p20f %s.txt", fileName)
			}

			err := Cmd(zipCmd)
			if err == nil {
				hadoopDir := fmt.Sprintf("%s/%s/%s/%s/", HdfsBaseDir, project, work.Date, work.Hour)
				mkdirCmd := fmt.Sprintf("source ~/.bashrc ; %s dfs -mkdir -p %s.txt.bz2 %s", HADOOP_BIN, fileName, hadoopDir)
				Cmd(mkdirCmd)

				for i := 0; i <= 10; i++ {
					uploadCmd := fmt.Sprintf("source ~/.bashrc ; %s dfs -put -f %s.txt.bz2 %s", HADOOP_BIN, fileName, hadoopDir)
					err = Cmd(uploadCmd)
					if err != nil {
						errorLog("upload error:" + uploadCmd + " error:" + err.Error())
						fmt.Sprintf("%d:upload error:%s;error:%s", i, uploadCmd, err.Error())
						time.Sleep(2 * time.Second)
					} else {
						RemoveFile(fileName + ".txt.bz2")
						infoLog("upload end : " + project)
						break
					}
				}
			} else {
				errorLog("zip error:" + zipCmd + " error:" + err.Error())
			}
			<-pNum
			rs <- true
		}(projectName, results)
	}

	for i := 0; i < work.ProjectNum; i++ {
		<-results
	}
	close(pNum)
}

func GetProjectList(baseDir string) []string {
	dirList := GetAllFilesByDir(baseDir)
	majorProject := map[string]bool{
		"hbtt":       true,
		"zhushou":    true,
		"xqlm2":      true,
		"browser":    true,
		"mbsearch":   true,
		"tianqiwang": true,
		"gsq":        true,
		"shoujilm":   true,
		"loginsdk":   true,
		"calendar":   true,
		"xqtt":       true,
	}
	projectList := []string{}
	for _, dir := range dirList {
		project := strings.TrimLeft(Substr(dir, strings.LastIndex(dir, "/"), len(dir)), "/")
		_, ok := majorProject[project]
		if isMajor == "yes" {
			if ok {
				projectList = append([]string{project}, projectList...)
			}
		} else {
			if !ok {
				projectList = append(projectList, project)
			}
		}
	}
	return projectList
}

func GetYearByDate(date string) int {
	tmpTm, err := time.ParseInLocation("20060102", strings.Replace(date, "-", "", -1), time.Local)
	if err != nil {
		return 2016
	}
	return tmpTm.Year()
}

/**
 *	获取文件目录
 *	@param string file
 *	@return string
 */
func GetFilePath(file string) string {
	if stat, err := os.Stat(file); err == nil && stat.IsDir() {
		return file
	}

	return filepath.Dir(file)
}

/**
 *	获取目录所有文件
 *	@param string dir
 *	@return slice
 */
func GetAllFilesByDir(dir string) []string {
	pattern := GetFilePath(dir) + "/*"
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil
	}
	return files
}

/**
 *	同步执行命令
 *	@return error
 */
func Cmd(cmdStr string) error {
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	cmd.Start()
	status := cmd.Wait()
	return status
}

// 同步执行命令, 并返回执行的结果
func Exec(cmdStr string) (string, error) {
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(bytes.TrimSpace(out)), nil
}

func Substr(s string, pos, length int) string {
	runes := []rune(s)
	l := pos + length
	if pos > len(runes) {
		return ""
	}
	if l > len(runes) {
		l = len(runes)
	}
	return string(runes[pos:l])
}

/**
 *	检查文件或目录是否存在
 *	@param string filename  文件或目录
 *	@return bool
 */
func IsExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

/**
 *	删除文件或空目录
 *	@param string filename 文件或目录
 *	@return error
 */
func RemoveFile(filename string) error {
	if IsExist(filename) {
		return os.Remove(filename)
	}

	return nil
}

/**
 *	获取目录指定匹配模式的所有文件
 *	@param string dir
 *	@param string pattern 匹配模式
 *	@return slice
 */
func GetAllFileByPattern(dir string, pattern string) []string {
	filePattern := GetFilePath(dir) + "/" + pattern
	files, err := filepath.Glob(filePattern)
	if err != nil {
		return nil
	}

	return files
}

func infoLog(msg string) {
	log := fmt.Sprintf("(info %s)：%s", time.Now().Format("2006-01-02 15:04:05"), msg)
	fmt.Println(log)
}
func errorLog(msg string) {
	log := fmt.Sprintf("(error %s)：%s", time.Now().Format("2006-01-02 15:04:05"), msg)
	fmt.Println(log)
}

//获取运算日期
func GetRunDate(format string) string {
	return time.Unix(time.Now().Unix()-int64(3600), 0).Format(format)
}
func FileSize(fileName string) int {
	if info, err := os.Stat(fileName); err == nil {
		return int(info.Size())
	}
	return 0
}

func Mkdir(dir string) (e error) {
	_, er := os.Stat(dir)
	b := er == nil || os.IsExist(er)
	if !b {
		if err := os.MkdirAll(dir, 0755); err != nil {
			if os.IsPermission(err) {
				fmt.Println("create dir error:", err.Error())
				e = err
			}
		}
	}
	return
}

//每小时小于10分钟的那次执行上传，否则只同步数据
func GetUploadState() string {
	minute := time.Now().Minute()
	if minute < 10 && minute > 0 {
		return "yes"
	} else {
		return "no"
	}
}
