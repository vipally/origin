package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	maxBtListLen = 50
	BtMe         = 2
)

func Bts() (bts string) {
	var buf bytes.Buffer
	var __pcs [maxBtListLen]uintptr
	n := runtime.Callers(BtMe, __pcs[0:])
	for i := 0; i < n; i++ {
		_pc := __pcs[i]
		_f := runtime.FuncForPC(_pc)
		_file, _line := _f.FileLine(_pc)
		s := fmt.Sprintf("#%d Func{%s} File{%s : %d} pc{%0X}\n",
			i, _f.Name(), _file, _line, _pc)
		buf.WriteString(s)
	}

	return buf.String()
}

func CallerService() string {
	const maxBtListLen = 50
	var __pcs [maxBtListLen]uintptr
	n := runtime.Callers(2, __pcs[0:])
	for i := 0; i < n; i++ {
		_pc := __pcs[i]
		_f := runtime.FuncForPC(_pc).Name()
		if strings.Contains(_f, "Service") {
			ss := strings.Split(_f, ".")
			if len(ss) == 3 {
				pack, obj := ss[0], ss[1]
				if strings.Contains(obj, "Service") {
					return obj[2 : len(obj)-1]
				} else {
					idx := strings.LastIndex(pack, "/")
					return pack[idx+1:]
				}
			}
		}
	}
	return "???" //+ Bts()
}

func OnRpcRely(nodeId int, method string, way string) {
	//fmt.Printf("service rely: %s nodeId=%d method=%s Bts=%s\n", way, nodeId, method, CallerService())
	relys.Push(CallerService(), method, way, nodeId)
}

const relyFile = "rpc_rely.json"

var relys relyMgr

func init() {
	relys.Init()
}

type relyObj struct {
	service     string
	relyService string
	relyMethod  string
}
type serviceList struct {
	Count int
	List  map[string]*relyServiceList
}

func (slf *serviceList) Init() {
	slf.List = make(map[string]*relyServiceList)
}

type relyServiceList struct {
	Count int
	List  map[string]*methodList
}

func (slf *relyServiceList) Init() {
	slf.List = make(map[string]*methodList)
}

type methodList struct {
	Count int
	List  map[string]bool
}

func (slf *methodList) Init() {
	slf.List = make(map[string]bool)
}

type relyMgr struct {
	ch   chan relyObj
	save serviceList //service->relyService->method+way
}

func (slf *relyMgr) Init() {
	slf.ch = make(chan relyObj, 10240)
	slf.save.Init()
	if b, err := ioutil.ReadFile(relyFile); err == nil {
		json.Unmarshal(b, &slf.save)
	}
	go slf.goWorker()
}

func (slf *relyMgr) Push(service, method, way string, nodeId int) {
	mm := strings.Split(method, ".")
	relyService, relyMethod := "??", "??"
	switch {
	case len(mm) >= 2:
		relyService, relyMethod = mm[0], mm[1]
	case len(mm) >= 1:
		relyMethod = mm[0]
	}
	var obj = relyObj{
		service:     service,
		relyService: relyService,
		relyMethod:  fmt.Sprintf("%s_%s_%d", relyMethod, way, nodeId),
	}
	slf.ch <- obj
}

func (slf *relyMgr) goWorker() {
	timer := time.NewTimer(time.Minute)
	update := false
	for {
		select {
		case <-timer.C:
			if update {
				update = false
				slf.SaveRely()
			}
		case obj := <-slf.ch:
			if slf.mergeObj(obj) {
				update = true
			}
		}
	}
}

func (slf *relyMgr) SaveRely() error {
	var save serviceList
	if b, err := ioutil.ReadFile(relyFile); err == nil {
		json.Unmarshal(b, save)
	}

	for service, p1 := range save.List {
		for relyService, p2 := range p1.List {
			for relyMethod, _ := range p2.List {
				obj := relyObj{
					service:     service,
					relyService: relyService,
					relyMethod:  relyMethod,
				}
				slf.mergeObj(obj)
			}
		}
	}

	b, _ := json.MarshalIndent(slf.save, "  ", "  ")
	err := ioutil.WriteFile(relyFile, b, os.ModePerm)
	return err
}

func (slf *relyMgr) mergeObj(obj relyObj) bool {
	update := false
	mp1, ok1 := slf.save.List[obj.service]
	if !ok1 {
		mp1 = &relyServiceList{}
		mp1.Init()
		slf.save.List[obj.service] = mp1
		slf.save.Count = len(slf.save.List)
		update = true
	}
	mp2, ok2 := mp1.List[obj.relyService]
	if !ok2 {
		mp2 = &methodList{}
		mp2.Init()
		mp1.List[obj.relyService] = mp2
		mp1.Count = len(mp1.List)
		update = true
	}
	_, ok3 := mp2.List[obj.relyMethod]
	if !ok3 {
		mp2.List[obj.relyMethod] = true
		mp2.Count = len(mp2.List)
		update = true
	}
	return update
}
