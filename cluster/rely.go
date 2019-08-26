package cluster

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
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
	return "???" + Bts()
}

func OnRpcRely(nodeId int, method string, way string) {
	fmt.Printf("service rely: %s nodeId=%d method=%s Bts=%s\n", way, nodeId, method, CallerService())
}
