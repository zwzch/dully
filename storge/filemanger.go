package storge

import (
	"sync"
	"strings"
	"os"
	"fmt"
	"time"
	"dully/conf"

)
var fm *FileManager
type FileManager struct {
	lock         *sync.RWMutex
	fileMap      *hashmap
	fileMaxSize  int64  //
	fileSequence []byte //
	md5map       *hashmap
	nameCache    *lruCache
	currFileName string
}
func OpenFileManager() (f *FileManager) {
	f = &FileManager{lock: new(sync.RWMutex), fileMap: NewHashMap(), fileMaxSize:conf.CF.MaxDataSize, md5map: NewHashMap(), nameCache:NewLruCache(1<<20)}
	f.getFData()
	return
}
func (this *FileManager)getFData() (fdata *Fdata) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if _filedata, ok := this.fileMap.Get(_current_file_); ok {

		filedata := _filedata.(*Fdata)
		if filedata.FileSize() < this.fileMaxSize {
			//如果文件大小小于配置的最大文件大小 返回当前文件
			return filedata
		}
	} else {
		//如果当前文件不存在 创建一个当前文件
		v, err := FDB.Get([]byte(_current_file_))
		var filename string
		if err == nil && v != nil {
			filename = string(v)
			fdata = this._openFdataFile(filename)
			this.fileMap.Put(filename,fdata)
			this.fileMap.Put(_current_file_,fdata)
			FDB.Put([]byte(_current_file_), []byte(filename))
			this.currFileName = filename
			return
		}
	}
	return this._newFdata(true)
	//返回一个新鲜的文件
}
func (this *FileManager) _openFdataFile(filename string) (fdata *Fdata) {
	idxfilename := strings.Replace(filename, ".dat", ".idx", -1)
	currFile, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	currIdxFile, err := os.OpenFile(idxfilename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err == nil {
		stat, _ := currFile.Stat()
		fdata = &Fdata{filename, stat.Size(), currFile, currIdxFile, new(sync.RWMutex), new(ReadBean)}
	} else {
		fmt.Println("error==>", err.Error())
	}
	return
}
func (this *FileManager) _newFdata(isCurrent bool) (fdata *Fdata) {
	sub := time.Now().Unix()
	//文件前缀时间戳
	//	fmt.Println("sub:", sub)
	FDB.Put([]byte(fmt.Sprint(_dat_, sub)), []byte{0})
	filename := fmt.Sprint(conf.CF.FileData, "/", sub, ".dat")
	fdata = this._openFdataFile(filename)
	this.fileMap.Put(fdata.FileName, fdata)
	if isCurrent {
		this.fileMap.Put(_current_file_, fdata)
		FDB.Put([]byte(_current_file_), []byte(fdata.FileName))
		this.currFileName = filename
	}
	return
}
func (this *FileManager) GetFdataByName(filename string) (fdata *Fdata) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	if _fdata, ok := this.fileMap.Get(filename); !ok {
		fdata = this._openFdataFile(filename)
		this.fileMap.Put(filename, fdata)
	} else {
		fdata = _fdata.(*Fdata)
	}
	return
}
func (this *FileManager) getSegment(fingerprint string) (segment Segment, err error) {
	//	this.lock.RLock()
	//	defer this.lock.RUnlock()
	s, b := this.nameCache.Get(fingerprint)
	if b && s != nil {
		s1 := s.(*Segment)
		segment = *s1
		return
	}
	segment, err = DBGetSegment([]byte(fingerprint))
	if err == nil {
		this.nameCache.Add(fingerprint, &segment)
	}
	return
}
func (this *FileManager) setNameCache(fingerprint string, segment *Segment) {
	this.nameCache.Add(fingerprint, segment)
}
func (this *FileManager) GetMd5Bean(md5key []byte) (mb *Md5Bean) {
	if _mb, ok := this.md5map.Get(string(md5key)); ok {
		mb = _mb.(*Md5Bean)
		return
	}
	nmb, err := DBGetMd5Bean(md5key)
	if err == nil {
		mb = &nmb
		this.md5map.Put(string(md5key), mb)
	}
	return
}
func (this *FileManager) removeNameCache(fingerprint string) {
	this.nameCache.Remove(fingerprint)
}
//注意这个删除并非线程安全，mk5key并发调用仍然存储相同md5值刚存储就被删除的情况
//由于正常情况下出现机率极低，此处不做处理
func (this *FileManager) DelMd5Bean(md5key []byte) {
	this.lock.Lock()
	defer this.lock.Unlock()
	//	delete(this.md5map, string(md5key))
	this.md5map.Del(string(md5key))
	DBDel(md5key)
	return
}