package storge

import "sync"

var lockString = &LockString{locks: make(map[string]*sync.Mutex, 0), lockCount: make(map[string]int32, 0), lock: new(sync.Mutex)}
type LockString struct {
	locks     map[string]*sync.Mutex
	lockCount map[string]int32
	lock      *sync.Mutex
}
func (this *LockString) Lock(s string) {
	this.lock.Lock()
	var lock *sync.Mutex
	var ok bool
	if lock, ok = this.locks[s]; !ok {
		lock = new(sync.Mutex)
		this.locks[s] = lock
		this.lockCount[s] = 1
	} else {
		this.lockCount[s] = this.lockCount[s] + 1
	}
	//	fmt.Println("LockString==>", this.lockCount[s])
	this.lock.Unlock()
	lock.Lock()
}

func (this *LockString) UnLock(s string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if lock, ok := this.locks[s]; ok {
		if this.lockCount[s] == 1 {
			//			fmt.Println("UnLock lockString==>", s)
			delete(this.locks, s)
			delete(this.lockCount, s)
		} else {
			this.lockCount[s] = this.lockCount[s] - 1
		}
		lock.Unlock()
	}
}
