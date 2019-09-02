package originnode

import (
	"fmt"
	"log"

	"github.com/duanhf2012/origin/util"

	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/duanhf2012/origin/cluster"
	"github.com/duanhf2012/origin/service"
	"github.com/duanhf2012/origin/sysmodule"
	"github.com/duanhf2012/origin/sysservice"
)

type CExitCtl struct {
	exitChan  chan bool
	waitGroup *sync.WaitGroup
}

type COriginNode struct {
	CExitCtl
	serviceManager     service.IServiceManager
	sigs               chan os.Signal
	debugListenAddress string
}

var initservicelist []service.IService

func InitService(iservice service.IService) {
	initservicelist = append(initservicelist, iservice)
}

func (s *COriginNode) Init() {

	s.SetupService(initservicelist...)

	//初始化全局模块
	logger := service.InstanceServiceMgr().FindService("syslog").(service.ILogger)
	ret := service.InstanceServiceMgr().Init(logger, s.exitChan, s.waitGroup)
	if ret == false {
		os.Exit(-1)
	}

	//检查所有的依赖Service是否可达
	if nErr := s.checkServicesRelys(); nErr > 0 {
		service.GetLogger().Printf(sysmodule.LEVER_FATAL, "checkServicesRelys: with %d error(s)", nErr)
		if cluster.DebugMode() { //调试模式配置有错误不让运行
			//os.Exit(-1)
		}
	}

	util.Log = logger.Printf
	s.sigs = make(chan os.Signal, 1)
	signal.Notify(s.sigs, syscall.SIGINT, syscall.SIGTERM)
}

// OpenDebugCheck ("localhost:6060")...http://localhost:6060/
func (s *COriginNode) OpenDebugCheck(listenAddress string) {
	s.debugListenAddress = listenAddress
}

func (s *COriginNode) SetupService(services ...service.IService) {
	ppService := &sysservice.PProfService{}
	services = append(services, ppService)
	cluster.InstanceClusterMgr().AddLocalService(ppService)
	for i := 0; i < len(services); i++ {
		services[i].Init(services[i])

		if cluster.InstanceClusterMgr().HasLocalService(services[i].GetServiceName()) == true {
			service.InstanceServiceMgr().Setup(services[i])
		} else {
			service.InstanceServiceMgr().AddNonLocalService(services[i]) //不是本地的service也把对象加进去 方便检查依赖
		}
	}
}

func (s *COriginNode) Start() {
	if s.debugListenAddress != "" {
		go func() {
			log.Println(http.ListenAndServe(s.debugListenAddress, nil))
		}()
	}

	//开始运行集群
	cluster.InstanceClusterMgr().Start()

	//开启所有服务
	service.InstanceServiceMgr().Start()

	//监听退出信号
	select {
	case <-s.sigs:
		service.GetLogger().Printf(sysmodule.LEVER_WARN, "Recv stop sig")
		fmt.Printf("Recv stop sig")
	}

	//停止运行程序
	s.Stop()
	service.GetLogger().Printf(sysmodule.LEVER_INFO, "Node stop run...")
}

//检查所有的service依赖是否可达
func (s *COriginNode) checkServicesRelys() int {
	nErr := 0

	mp := cluster.InstanceClusterMgr().GetAllConfigServiceList()
	list := service.InstanceServiceMgr().GetAllService()
	for _, v := range list {
		name := v.GetServiceName()
		if _, ok := mp[name]; !ok {
			service.GetLogger().Printf(sysmodule.LEVER_WARN, "[originCheck 6] checkServicesRelys: service %s does not exists in cluster.json", name)
		}
	}

	cluster.SetAllowDuplicateServices(service.InstanceServiceMgr().GetAllowDuplicateService())
	nodes := cluster.InstanceClusterMgr().GetAllNodeList()
	for _, nodeId := range nodes {
		reachable := cluster.InstanceClusterMgr().GetAllReachableServices(nodeId)
		if list := cluster.InstanceClusterMgr().GetNodeServiceList(nodeId); list != nil {
			for _, name := range list {
				svs := service.InstanceServiceMgr().FindNonLocalService(name)
				if svs == nil {
					service.GetLogger().Printf(sysmodule.LEVER_ERROR, "[originCheck 4] checkServicesRelys: service %s at node %d does not exists", name, nodeId)
					nErr++
					continue
				}
				relys := svs.GetDeepRelyServices()
				for rely, _ := range relys {
					if _, ok := reachable[rely]; !ok {
						service.GetLogger().Printf(sysmodule.LEVER_ERROR, "[originCheck 5] checkServicesRelys: rely service %s->%s at node %d is non-reachable", name, rely, nodeId)
						nErr++
					}
				}
			}
		}
	}

	return nErr
}

func (s *COriginNode) Stop() {
	close(s.exitChan)
	s.waitGroup.Wait()
}

func NewOriginNode() *COriginNode {

	//创建模块
	node := new(COriginNode)
	node.exitChan = make(chan bool)
	node.waitGroup = &sync.WaitGroup{}

	//安装系统服务
	syslogservice := &sysservice.LogService{}
	syslogservice.InitLog("syslog", sysmodule.LEVER_INFO)

	service.InstanceServiceMgr().Setup(syslogservice)

	//初始化集群对象
	err := cluster.InstanceClusterMgr().Init()
	if err != nil {
		fmt.Print(err)
		os.Exit(-1)
		return nil
	}

	return node
}

func (s *COriginNode) GetSysLog() *sysservice.LogService {
	logService := service.InstanceServiceMgr().FindService("syslog")
	if logService == nil {
		fmt.Printf("Cannot find syslog service!")
		os.Exit(-1)
		return nil
	}

	return logService.(*sysservice.LogService)
}
