package storge

import (
	"io/ioutil"
	"dully/conf"
	"fmt"
	"time"
	"os"
	"strings"
	"sync"
)
type Fdata struct {
	FileName string   //所在文件名
	CurPoint int64    //当前指针
	f        *os.File //
	idxf     *os.File //
	lock     *sync.RWMutex
	rb       *ReadBean
}
func (this *Fdata) FileSize() int64 {
	return this.CurPoint
}
func (this *Fdata) Compact(chip int32) (finish bool) {
	defer catchError()
	if this.f.Name() == fm.currFileName || this.FileSize() > int64(chip*10) || this.rb.rps > conf.CF.ReadPerSecond {
		return
	}
	return this.strongCompact(chip)
}
func (this *Fdata) strongCompact(chip int32) (finish bool) {
	defer catchError()
	//	fmt.Println("Compact:", this)
	bs, err := ioutil.ReadFile(this.idxf.Name())
	if err == nil {
		newfdata := fm._newFdata(false)
		length := len(bs) / 16
		finish = true
		for i := 0; i < length; i++ {
			md5key := bs[i*16 : (i+1)*16]
			mb, err := DBGetMd5Bean(md5key)
			if err == nil {
				bs, err := this.GetData(&mb)
				if  err== nil {
					err = _AppendData(bs, newfdata)
				}
				if err != nil {
					finish = false
				}
			} else {
				fmt.Println("no md5key")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if finish {
			fmt.Println("compact file: ", this.f.Name(), ">>>>>>", newfdata.f.Name())
			fmt.Println("compact size: ", this.FileSize(), ">>>>>>", newfdata.FileSize())
			this.CloseAndDelete()
		}
	}
	return
}
func (this *Fdata) CloseAndDelete() {
	fmt.Println("CloseAndDelete:", this)
	this.lock.Lock()
	defer this.lock.Unlock()
	defer catchError()
	filename := this.f.Name()
	idxfilename := this.idxf.Name()
	this.f.Close()
	this.idxf.Close()
	os.Remove(filename)
	os.Remove(idxfilename)
	FDB.Del([]byte(fmt.Sprint(_dat_, filename[len(conf.CF.FileData)+1:strings.Index(filename, ".")])))
	return
}

func (this *Fdata) GetData(md5Bean *Md5Bean) (bs []byte, err error) {
	//	this.lock.RLock()
	//	defer this.lock.RUnlock()
	bs, err = ReadAt(this.f, int(md5Bean.Size), md5Bean.Offset)
	if md5Bean.Compress {
		bs, err = compresseDecode(bs)
	}
	this.rb.add()
	return
}
func (this *Fdata) WriteIdxMd5(md5key []byte) (err error) {
	_, err = Write(this.idxf, md5key)
	return
}
func (this *Fdata) AppendData(bs []byte) (offset int64, size int32, err error) {
	//	fmt.Println("AppendData==>", this.f.Name(), " ,", len(bs), " ,", offset)
	if conf.CF.Compress {
		bs = compresseEncode(bs)
	}
	size = int32(len(bs))
	offset = this.GetAndSetCurPoint(int64(size))
	_, err = Append(this.f, bs, offset)
	if err != nil {
		panic(_ERR_CODE_APPEND_DATA)
	}
	return
}
func (this *Fdata) GetAndSetCurPoint(size int64) (offset int64) {
	this.lock.Lock()
	defer this.lock.Unlock()
	offset = this.CurPoint
	this.CurPoint = offset + size
	return
}
