package storge

import (
	"dully/db"
	"dully/conf"
			"os"
	"time"
	"sync/atomic"
	"strings"
	"fmt"
		"encoding/gob"
	"bytes"
	"github.com/golang/snappy"
	"crypto/md5"
	"runtime/debug"
	"errors"
	"hash/crc32"
	"encoding/hex"
)
var _current_file_ = "_current_file_"
var _dat_ = "_dat_"
var _del_ = "_del_"
var _ERR_CODE_APPEND_DATA = 501
var FDB *db.DB
/////////////////////////////////////
/////// storge init /////////////////
/////////////////////////////////////
func Init() {
	fmt.Println(conf.CF.FileData+"/fsdb")
	FDB = db.NewDB(conf.CF.FileData+"/fsdb", true)
	fm = OpenFileManager()
	go _Ticker(1800, _compact)
}

/////////////////////////////////////
/////// md5 /////////////////
/////////////////////////////////////
type Md5Bean struct {
	Offset   int64  //文件所在位置
	Size     int32  //文件大小 字节
	FileName string //所在文件名
	QuoteNum int32  //引用数
	Sequence []byte //文件序号
	Compress bool   //是否压缩
}
func (this *Md5Bean) AddQuote() {
	atomic.AddInt32(&this.QuoteNum, 1)
}
func (this *Md5Bean) SubQuote() {
	atomic.AddInt32(&this.QuoteNum, -1)
}
func _saveDel(mb *Md5Bean) {
	filename := mb.FileName
	lockString.Lock(filename)
	defer lockString.UnLock(filename)
	size := mb.Size
	filekey := fmt.Sprint(_del_, filename)
	v, err := FDB.Get([]byte(filekey))
	if err == nil {
		i := Bytes2Octal(v) + size
		FDB.Put([]byte(filekey), Octal2bytes(i))
	} else {
		FDB.Put([]byte(filekey), Octal2bytes(size))
	}
}
func DBGetMd5Bean(md5key []byte) (md5 Md5Bean, err error) {
	var v []byte
	v, err = FDB.Get(md5key)
	if err == nil {
		md5 = DecoderMd5(v)
	}
	return
}
func MD5(data []byte) []byte {
	m := md5.New()
	m.Write(data)
	return m.Sum(nil)
}
func DecoderMd5(data []byte) (md5 Md5Bean) {
	var network bytes.Buffer
	_, er := network.Write(data)
	dec := gob.NewDecoder(&network)
	if er == nil {
		er = dec.Decode(&md5)
	}
	return
}
func NewMd5Bean(offset int64, size int32, filename string, sequence []byte, compress bool) (mb *Md5Bean) {
	mb = &Md5Bean{Offset: offset, Size: size, QuoteNum: 0, FileName: filename, Sequence: sequence, Compress: compress}
	return
}

func DBPutMd5Bean(md5key []byte, md5Bean Md5Bean) (err error) {
	err = FDB.Put(md5key, EncodeMd5(md5Bean))
	return
}
func EncodeMd5(md5 Md5Bean) (bs []byte) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	enc.Encode(md5)
	bs = network.Bytes()
	return
}

/////////////////////////////////////
/////// ReadBean /////////////////
/////////////////////////////////////
type ReadBean struct {
	rps          int64 //read per second
	lastReadTime int64
}
func (this *ReadBean) add() {
	if time.Now().Unix()-this.lastReadTime < 60 {
		atomic.AddInt64(&this.rps, 1)
	} else {
		atomic.StoreInt64(&this.rps, 1)
	}
	atomic.StoreInt64(&this.lastReadTime, time.Now().Unix())
}

/////////////////////////////////////
/////// Segment /////////////////
/////////////////////////////////////

type Segment struct {
	Id        int64  //文件ID号
	Name      string //文件名
	FileType  string //文件类型
	Md5       []byte //文件md5值
	ShardName string
}
func NewSegment(name string, filetype string, md5 []byte, shardname string) (s *Segment) {
	//	fmt.Println(name, " | ", filetype)
	s = new(Segment)
	s.Name = name
	s.FileType = filetype
	s.Md5 = md5
	s.ShardName = shardname
	return
}

func EncodeSegment(segment Segment) (bs []byte) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	enc.Encode(segment)
	bs = network.Bytes()
	return
}
func DBGetSegment(key []byte) (s Segment, err error) {
	var v []byte
	v, err = FDB.Get(key)
	if err == nil {
		s = DecoderSegment(v)
	}
	return
}
func DecoderSegment(data []byte) (segment Segment) {
	var network bytes.Buffer
	_, er := network.Write(data)
	dec := gob.NewDecoder(&network)
	if er == nil {
		er = dec.Decode(&segment)
	}
	return
}
func DBPutSegment(key []byte, s Segment) (err error) {
	err = FDB.Put(key, EncodeSegment(s))
	return
}
/////////////////////////////////////
/////// Basic Utils /////////////////
/////////////////////////////////////
func Append(f *os.File, bs []byte, offset int64) (n int, err error) {
	n, err = f.WriteAt(bs, offset)
	f.Sync()
	return
}
func Write(f *os.File, bs []byte) (n int, err error) {
	n, err = f.Write(bs)
	f.Sync()
	return
}
func ReadAt(f *os.File, byteInt int, offset int64) (bs []byte, err error) {
	bs = make([]byte, byteInt)
	_, err = f.ReadAt(bs, offset)
	return
}

func catchError(msg ...string) {
	if err := recover(); err != nil {
		if msg != nil {
			fmt.Println(strings.Join(msg, ","), err)
		}
	}
}
func Bytes2Octal(bb []byte) (value int32) {
	value = int32(0x0000)
	for i, b := range bb {
		ii := uint(b) << uint((3-i)*4)
		value = value | int32(ii)
	}
	return
}
func compresseEncode(src []byte) []byte {
	return snappy.Encode(nil, src)
}
func compresseDecode(src []byte) (bs []byte, err error) {
	return snappy.Decode(nil, src)
}
func Octal2bytes(row int32) (bs []byte) {
	bs = make([]byte, 0)
	for i := 0; i < 4; i++ {
		r := row >> uint((3-i)*4)
		bs = append(bs, byte(r))
	}
	return
}

func _fingerprint(bs []byte) (dest string) {
	ieee := crc32.NewIEEE()
	ieee.Write(bs)
	return hex.EncodeToString(ieee.Sum(nil))
}
func DBDel(key []byte) (err error) {
	err = FDB.Del(key)
	return
}
/////////////////////////////////////
/////// compact /////////////////
/////////////////////////////////////
func _Ticker(second int, function func()) {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	for {
		time.Sleep(time.Duration(second) * time.Second)
		function()
	}
}
func _compact() {
	catchError()
	m := FDB.GetIterLimit(_del_, fmt.Sprint(_del_, "z"))
	if m != nil {
		for k, v := range m {
			chip := Bytes2Octal(v)
			//			fmt.Println("scan compact key:", k, " ", chip)
			filename := strings.Replace(k, _del_, "", -1)
			fdata := fm.GetFdataByName(filename)
			if fdata.Compact(chip) {
				FDB.Del([]byte(k))
			}
		}
	}
}


/////////////////////////////////////
/////// AppendData /////////////////
/////////////////////////////////////
func AppendData(bs []byte, name string, fileType string, shardname string) (err error) {
	//	defer catchError()
	defer func() {
		if er := recover(); er != nil {
			fmt.Println(string(debug.Stack()))
		}
	}()
	if conf.CF.Readonly {
		return errors.New("readonly")
	}
	if name == "" || bs == nil || len(bs) == 0 {
		return errors.New("nil")
	}
	fingerprint := _fingerprint([]byte(name))
	lockString.Lock(fingerprint)
	defer lockString.UnLock(fingerprint)
	//lock crc32循环校验
	if len(shardname) > 0 {
		return DBPutSegment([]byte(fingerprint), *NewSegment(name, fileType, nil, shardname))
	}
	md5key := MD5(bs)
	//取md5作为key
	sbs := NewSegment(name, fileType, md5key, "")
	DBPutSegment([]byte(fingerprint), *sbs)
	//levelDB中存储segment
	fm.setNameCache(fingerprint, sbs)
	//lruCache中存储segment
	lockString.Lock(string(md5key))
	defer lockString.UnLock(string(md5key))
	mb := fm.GetMd5Bean(md5key)
	if mb == nil {
		//如果md5bean是空 表明当前文件不存在
		f := fm.getFData()
		//获取文件
		offset, size, er := f.AppendData(bs)
		//文件附加内容
		if er != nil {
			return er
		}
		mb = NewMd5Bean(offset, size, f.FileName, nil, conf.CF.Compress)
		err = f.WriteIdxMd5(md5key)
		//idex文件中写入md5key
		if err != nil {
			return
		}
	}
	mb.AddQuote()
	fmt.Println("append:", name)
	fmt.Println("quote===>", mb.QuoteNum)
	err = DBPutMd5Bean(md5key, *mb)
	//插入md5信息防止重复append
	return
}
/////////////////////////////////////
/////// Triger AppendData ///////////
/////////////////////////////////////
func _AppendData(bs []byte, f *Fdata) (err error) {
	offset, size, er := f.AppendData(bs)
	if er != nil {
		return er
	}
	md5key := MD5(bs)
	//	offset := f.GetAndSetCurPoint(int64(len(bs)))
	mb := NewMd5Bean(offset, size, f.FileName, nil, conf.CF.Compress)
	err = f.WriteIdxMd5(md5key)
	if err != nil {
		return
	}
	mb.AddQuote()
	err = DBPutMd5Bean(md5key, *mb)
	return
}

/////////////////////////////////////
/////// GetData /////////////////
/////////////////////////////////////
func GetData(name string) (bs []byte, shardname string, er error) {
	//	defer catchError()
	defer func() {
		if er := recover(); er != nil {
			fmt.Println(string(debug.Stack()))
		}
	}()
	if name == "" {
		return nil, "", errors.New("nil")
	}
	fingerprint := _fingerprint([]byte(name))
	//根据文件的key获得对应的crc值
	segment, err := fm.getSegment(fingerprint)
	if err == nil {
		shardname = segment.ShardName
		if len(shardname) > 0 {
			return
		}
		md5key := segment.Md5
		md5Bean, err := DBGetMd5Bean(md5key)
		if err == nil {
			filename := md5Bean.FileName
			//			fmt.Println("GetData:", filename)
			fdata := fm.GetFdataByName(filename)
			bs, er = fdata.GetData(&md5Bean)
			//			bs = bs[:len(bs)-8]
		}
	} else {
		er = err
	}
	return
}
/////////////////////////////////////
/////// GetData /////////////////
/////////////////////////////////////
func DelData(name string) (shardname string, err error) {
	defer catchError()
	if conf.CF.Readonly {
		return "", errors.New("readonly")
	}
	if name == "" {
		return "", errors.New("nil")
	}
	fingerprint := _fingerprint([]byte(name))
	lockString.Lock(fingerprint)
	defer lockString.UnLock(fingerprint)
	segment, er := fm.getSegment(fingerprint)
	fm.removeNameCache(fingerprint)
	err = DBDel([]byte(fingerprint))
	//缓存和数据库中删除信息
	if er == nil {
		shardname = segment.ShardName
		md5key := segment.Md5
		if len(md5key) > 0 {
			lockString.Lock(string(md5key))
			defer lockString.UnLock(string(md5key))
			mb := fm.GetMd5Bean(md5key)
			if mb != nil {
				mb.SubQuote()
				if mb.QuoteNum <= 0 {
					fm.DelMd5Bean(md5key)
					_saveDel(mb)
				} else {
					DBPutMd5Bean(md5key, *mb)
				}
			}
		}
	}
	return
}