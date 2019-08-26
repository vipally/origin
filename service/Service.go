package service

import (
	"fmt"
	"sync"

	"reflect"
	"strings"
)

type MethodInfo struct {
	Fun       reflect.Value
	ParamList []reflect.Value
	types     reflect.Type
}

type IService interface {
	Init(Iservice IService) error
	OnInit() error
	OnRun() bool
	OnFetchService(iservice IService) error
	OnSetupService(iservice IService)  //其他服务被安装
	OnRemoveService(iservice IService) //其他服务被安装

	GetServiceName() string
	SetServiceName(serviceName string) bool
	GetServiceId() int

	GetStatus() int
	IsInit() bool
}

type BaseService struct {
	BaseModule

	//RelyServices map[string]int //依赖的服务(有RPC需求) ServicesName->relyDepth

	serviceid   int
	servicename string
	Status      int
}

// //定义直接依赖的Service 由具体实现Service提供
// func (slf *BaseService) DeclareRelyServices() []string {
// 	return []string{}
// }

// //添加依赖的服务声明
// func (slf *BaseService) AddRelyService(depth int, relys ...string) int {
// 	if slf.RelyServices == nil {
// 		slf.RelyServices = map[string]int{}
// 	}
// 	for _, v := range relys {
// 		if depth, ok := slf.RelyServices[v]; ok {
// 			GetLogger().Printf(LEVER_INFO, "BaseService.AddRelyService %s->%s fail:exists(depth %d)", slf.servicename, v, depth)
// 			continue
// 		}
// 		slf.RelyServices[v] = depth
// 	}
// 	return 0
// }

// //检查依赖的服务是否已声明
// func (slf *BaseService) CheckRelyService(rely string) bool {
// 	if depth, ok := slf.RelyServices[rely]; ok {
// 		return depth == 1
// 	}
// 	return false
// }

// //收集深度依赖的services
// func (slf *BaseService) DeepCollectRelyServices() error {
// 	relys := slf.DeclareRelyServices()
// 	for _, v := range relys {
// 		service := InstanceServiceMgr().FindService(v)
// 		if service == nil {
// 			GetLogger().Printf(LEVER_INFO, "BaseService.DeepCollectRelyServices %s do not find service %s", slf.servicename, v)
// 			continue
// 		}

// 	}
// 	return nil
// }

// // //打印依赖的服务
// // func (slf *BaseService) ShowRelyServices() string {
// // 	return ""
// // }

func (slf *BaseService) GetServiceId() int {
	return slf.serviceid
}

func (slf *BaseService) GetServiceName() string {
	return slf.servicename
}

func (slf *BaseService) SetServiceName(serviceName string) bool {
	slf.servicename = serviceName
	return true
}

func (slf *BaseService) GetStatus() int {
	return slf.Status
}

func (slf *BaseService) OnFetchService(iservice IService) error {
	return nil
}

func (slf *BaseService) OnSetupService(iservice IService) {
}

func (slf *BaseService) OnRemoveService(iservice IService) {
}

func (slf *BaseService) Init(iservice IService) error {
	slf.ownerService = iservice

	if iservice.GetServiceName() == "" {
		slf.servicename = fmt.Sprintf("%T", iservice)
		parts := strings.Split(slf.servicename, ".")
		if len(parts) != 2 {
			GetLogger().Printf(LEVER_ERROR, "BaseService.Init: service name is error: %q", slf.servicename)
			err := fmt.Errorf("BaseService.Init: service name is error: %q", slf.servicename)
			return err
		}

		slf.servicename = parts[1]
	}

	slf.serviceid = InstanceServiceMgr().GenServiceID()
	slf.BaseModule.rwModuleLocker = &sync.Mutex{}

	return nil
}
