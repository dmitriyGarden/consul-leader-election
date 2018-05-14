package election

import (
	"testing"

	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/testutil"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	serviceName  = "test.service"
	healthPrefix = "health"
	clucterSize  = 3
	serviceNum   = 10
)

type item struct {
	e *Election
	i int
}

func TestNewElection(t *testing.T) {

	log.Println("================== Create claster of consul =====================")
	servers := makeCluster(t)

	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()

	log.Println("================== Create services =====================")
	services := makeServices(t, servers)

	log.Println("================== Start processing without health checks =====================")
	counter := serviceNum * 2
	mutex := &sync.Mutex{}
	for _, i := range services {
		go func(i *item) {
			defer func() {
				mutex.Lock()
				counter--
				mutex.Unlock()
			}()
			i.e.Init()
		}(i)
		// The second process should not be started
		go func(i *item) {
			defer func() {
				mutex.Lock()
				counter--
				mutex.Unlock()
			}()
			i.e.Init()
		}(i)
	}
	time.Sleep(5 * time.Second)
	mutex.Lock()
	if counter != serviceNum {
		t.Errorf("There are %d gorutines instead %d", counter, serviceNum)
	}
	mutex.Unlock()

	if !mustNotLeader(services) {
		t.Errorf("Must not be a leader. We have no available healthchecks.")
	}
	log.Println("================== Create health checks =====================")
	// Service registration
	serviceRegister(t, services, servers)
	cl := waitForLeader(services)
	if cl != 1 {
		t.Errorf("There are %d leaders instead 1 after init services", cl)
	}
	log.Println("================== Try to re-election =====================")
	// Try to re-election
	err := services[0].e.ReElection()
	if err != nil {
		t.Error(err)
	}
	cl = waitForLeader(services)
	if cl != 1 {
		t.Errorf("There are %d leaders instead 1 after re-election", cl)
	}

	log.Println("================== Try to stop leader health check =====================")
	stopLeaderHelthCheck(t, services, servers)

	log.Println("================== Try to stop a leader =====================")
	// Try to stop leader
	for _, i := range services {
		if i.e.IsLeader() {
			i.e.Stop()
			time.Sleep(1 * time.Second)
			if i.e.IsLeader() {
				t.Errorf("%sElector is steel a leader after stop election", i.e.LogPrefix)
			}
			break
		}
	}
	if serviceNum > 1 {
		cl = waitForLeader(services)
		if cl != 1 {
			t.Errorf("There are %d leaders instead 1 after stop leader", cl)
		}
	}

	log.Println("================== Try to stop a consul server =====================")
	stopConsulServiceWithLeader(t, services, servers)

	log.Println("================== Stop all processes =====================")
	// Stop processing
	for _, i := range services {
		i.e.Stop()
	}
	if !mustNotLeader(services) {
		t.Errorf("Must not be a leader. We stop all processes")
	}
}

func stopLeaderHelthCheck(t *testing.T, items []*item, servers []*testutil.TestServer) {
	for i := range items {
		if items[i].e.IsLeader() {
			servers[items[i].i].AddCheck(t, getHID(i), getSrID(i), "critical")
			time.Sleep(6 * time.Second)
			if items[i].e.IsLeader() {
				t.Error("Elector is steel a leader after disable health check")
			}
			servers[items[i].i].AddCheck(t, getHID(i), getSrID(i), "passing")
			break
		}
	}
	cl := waitForLeader(items)
	if cl != 1 {
		t.Errorf("There are %d leaders instead 1 after disable health check", cl)
	}

}

func waitForLeader(items []*item) int {
	for i := 1; i < 12; i++ {
		time.Sleep(5 * time.Second)
		for _, it := range items {
			if it.e.IsLeader() {
				time.Sleep(5 * time.Second)
				return countLeader(items)
			}
		}
	}
	return countLeader(items)
}

func stopConsulServiceWithLeader(t *testing.T, items []*item, servers []*testutil.TestServer) {
	var cl int
	sr := map[int]struct{}{}
	for _, i := range items {
		sr[i.i] = struct{}{}
	}
	if len(sr) < 2 {
		cl = 0
	}
	for _, i := range items {
		if i.e.IsLeader() {
			servers[i.i].Stop()
		}
	}
	if len(sr) > 1 && serviceNum > 1 {
		cl = waitForLeader(items)
		if cl != 1 {
			t.Errorf("There are %d leaders instead 1 after stop consul node", cl)
		}
	}
}

func serviceRegister(t *testing.T, items []*item, servers []*testutil.TestServer) {
	for j, i := range items {
		servID := getSrID(j)
		hID := getHID(j)
		servers[i.i].AddService(t, servID, "passing", []string{})
		servers[i.i].AddCheck(t, hID, servID, "passing")
	}
}

func getHID(i int) string {
	return fmt.Sprintf("%s:%d", healthPrefix, i)
}
func getSrID(i int) string {
	return fmt.Sprintf("%s:%d", serviceName, i)
}

func countLeader(services []*item) int {
	c := 0
	for _, i := range services {
		if i.e.IsLeader() {
			c++
		}
	}
	return c
}

func mustNotLeader(services []*item) bool {
	for _, i := range services {
		if i.e.IsLeader() {
			return false
		}
	}
	return true
}

func makeServices(t *testing.T, servers []*testutil.TestServer) []*item {
	s := rand.NewSource(time.Now().Unix())
	r := rand.New(s)
	services := make([]*item, serviceNum)
	serversCount := len(servers)
	conf := api.DefaultConfig()
	for i := 0; i < serviceNum; i++ {
		srv := r.Intn(serversCount)
		conf.Address = servers[srv].HTTPAddr
		client, err := api.NewClient(conf)
		if err != nil {
			t.Fatal(err)
		}
		services[i] = &item{
			NewElection(client, []string{getHID(i)}, serviceName),
			srv,
		}
		services[i].e.SetLogLevel(LogDebug)
		services[i].e.LogPrefix = fmt.Sprintf("[EL-%d]", i)
	}
	return services
}

func makeCluster(t *testing.T) []*testutil.TestServer {
	var err error
	servers := make([]*testutil.TestServer, clucterSize)
	// Create a cluster of servers
	for i := 0; i < clucterSize; i++ {
		if i == 0 {
			servers[i], err = testutil.NewTestServerConfig(func(c *testutil.TestServerConfig) {
				c.LogLevel = "err"
			})
		} else {
			servers[i], err = testutil.NewTestServerConfig(func(c *testutil.TestServerConfig) {
				c.LogLevel = "err"
				c.Bootstrap = false
			})
			if err == nil {
				servers[0].JoinLAN(t, servers[i].LANAddr)
			}
		}
		if err != nil {
			for j := range servers {
				if servers[j] != nil {
					servers[j].Stop()
				}
			}
			t.Fatal(err)
		}
	}
	return servers
}
