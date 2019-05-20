package net

import (
		"sync"
	"sync/atomic"
	"dully/storge"
	"strings"
	"fmt"
)
var Factory *SlaveFactory
type SlaveBean struct {
	Name        string
	Addr        string
	Weight      int32
	WeightCount int32
}
type SlaveFactory struct {
	mu *sync.RWMutex
	//	slaveChan  chan string
	slaveMap   map[string]*SlaveBean
	slavevalid map[string]byte
	slaveSlb   map[string]byte
	readint    int32
}
func (this *SlaveFactory) getSlaveByWeight() (sb *SlaveBean) {
	this.mu.Lock()
	defer this.mu.Unlock()
	atomic.AddInt32(&this.readint, 1)
	atomic.CompareAndSwapInt32(&this.readint, 1<<31-1, 1)
LOOP:
	if len(this.slaveSlb) > 0 {
		c := this.readint % int32(len(this.slaveSlb))
		i := int32(0)
		name := ""
		for k, _ := range this.slaveSlb {
			if i == c {
				name = k
				break
			}
			i++
		}
		delete(this.slaveSlb, name)
		//		name := <-this.slaveChan
		//		var ok bool
		if _, ok := this.slavevalid[name]; !ok {
			goto LOOP
		}
		if _sb, ok := this.slaveMap[name]; ok {
			//			if sb.WeightCount > 0 {
			//				go func() {
			//					this.slaveChan <- sb.Name
			//				}()
			//			} else {
			//				atomic.StoreInt32(&sb.WeightCount, sb.Weight)
			//				goto LOOP
			//			}
			if _sb.WeightCount <= 0 {
				goto LOOP
			} else {
				atomic.AddInt32(&_sb.WeightCount, -1)
				return _sb
			}
		} else {
			goto LOOP
		}
	} else {
		//		for name, _ := range this.slavevalid {
		//			go func() {
		//				this.slaveChan <- name
		//			}()
		//		}
		//		if len(this.slaveChan) > 0 {
		//			goto LOOP
		//		}

		for k, _ := range this.slavevalid {
			if v, ok := this.slaveMap[k]; ok {
				if ok && v.WeightCount > 0 {
					this.slaveSlb[k] = 0
				}
			}
		}

		if len(this.slaveSlb) > 0 {
			goto LOOP
		} else if len(this.slaveMap) > 0 {
			for k, v := range this.slaveMap {
				v.WeightCount = v.Weight
				if _, ok := this.slavevalid[k]; ok {
					this.slaveSlb[k] = 0
				}
			}
			if len(this.slaveSlb) > 0 {
				goto LOOP
			}
		}
	}
	return
}
func AppendData(bs []byte, name string, fileType string) (err error) {
	err = storge.AppendData(bs, name, fileType, "")
	//sb := Factory.getSlaveByWeight()
	//if sb == nil || sb.Name == "master" {
	//	err = storge.AppendData(bs, name, fileType, "")
	//} else {
		//_, err = wfsPost(sb.Addr, bs, name, fileType)
		//if err == nil {
		//	err = storge.AppendData(bs, name, fileType, sb.Name)
		//} else {
		//	Factory._invalid(sb.Name)
		//	err = storge.AppendData(bs, name, fileType, "")
		//}
	//}
	return
}

func DelData(name string) (er error) {
	shardname, err := storge.DelData(name)
	if err == nil {
		if len(shardname) > 0 {
			addr := Factory.getAddrByName(shardname)
			_, er = wfsDel(addr, name)
		}
	}
	return
}

func GetData(uri string) (retbs []byte, err error) {
	return getDataByName(uri[3:])
}
func getDataByName(uri string) (retbs []byte, err error) {
	uri3 := uri
	name := uri3
	arg := ""
	if strings.Contains(uri3, "?") {
		index := strings.Index(uri3, "?")
		name = uri3[:index]
		arg = uri3[index:]
	}
	bs, shardname, err := storge.GetData(name)
	//	fmt.Println(len(bs), "  ", shardname)
	fmt.Println(arg)
	if err == nil && bs != nil {
		//if strings.HasPrefix(arg, "?imageView2") {
		//	spec := NewSpec(bs, arg)
		//	retbs = spec.GetData()
		//} else {
		//	retbs = bs
		//}
		retbs = bs
	} else if len(shardname) > 0 {
		//addr := Factory.getAddrByName(shardname)
		//if addr != "" {
		//	wf, er := wfsRead(addr, uri)
		//	if er == nil {
		//		retbs = wf.GetFileBody()
		//	} else {
		//		err = er
		//	}
		//} else {
		//	fmt.Println("err:", shardname, " is not exist")
		//}
	}
	return
}
//func NewSpec(bs []byte, arg string) (spec *Spec) {
//	spec = new(Spec)
//	spec.src = bs
//	//	fmt.Println("arg===>", arg)
//	if strings.HasPrefix(arg, "?imageView2") {
//		ss := strings.Split(arg, "/")
//		if ss != nil && len(ss) > 3 {
//			spec.Mode = GetMode(atoi(ss[1]))
//			switch ss[2] {
//			case "w":
//				spec.Width = atoi(ss[3])
//			case "h":
//				spec.Height = atoi(ss[3])
//			}
//			if len(ss) > 5 {
//				switch ss[4] {
//				case "w":
//					spec.Width = atoi(ss[5])
//				case "h":
//					spec.Height = atoi(ss[5])
//				}
//			}
//		}
//	}
//	return
//}
//type Spec struct {
//	RT     image.ResizeType
//	Mode   image.Mode
//	Width  int
//	Height int
//	src    []byte
//}



