package election

// Leader election
// https://www.consul.io/docs/guides/leader-election.html
import (
	"github.com/hashicorp/consul/api"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	LogDisable = iota
	LogError
	LogInfo
	LogDebug
)

type Election struct {
	Client       *api.Client // Consul client
	Checks       []string    // Slice of associated health checks
	leader       bool        // Flag of leader
	Kv           string      // Key in Consul kv
	sessionID    string      // Id session
	logLevel     uint8       //  Log level LogDisable|LogError|LogInfo|LogDebug
	inited       bool        // Flag of init.
	mutex        *sync.Mutex
	CheckTimeout time.Duration
	LogPrefix    string
}

// Check leader
func (e *Election) IsLeader() bool {
	return e.leader
}

// Log level LogDisable|LogError|LogInfo|LogDebug
func (e *Election) SetLogLevel(level uint8) {
	e.logLevel = level
}

// Params: Consul client, slice of associated health checks, service name
func NewElection(c *api.Client, checks []string, service string) *Election {
	e := &Election{
		Client:       c,
		Checks:       append(checks, "serfHealth"),
		leader:       false,
		Kv:           "services/" + strings.Replace(service, ".", "/", -1) + "/leader",
		mutex:        &sync.Mutex{},
		CheckTimeout: 5 * time.Second,
		LogPrefix:    "[EL] ",
	}
	return e
}

func (e *Election) createSession() (err error) {
	ses := &api.SessionEntry{
		Checks: e.Checks,
	}
	e.sessionID, _, err = e.Client.Session().Create(ses, nil)
	if err != nil {
		e.logError("Create session error " + err.Error())
	}
	return
}

func (e *Election) checkSession() (bool, error) {

	if e.sessionID == "" {
		return false, nil
	}
	res, _, err := e.Client.Session().Info(e.sessionID, nil)

	if err != nil {
		e.logError("Info session error " + err.Error())
	}

	return res != nil, err
}

// Try to acquire
func (e *Election) acquire() (bool, error) {
	kv := &api.KVPair{
		Key:     e.Kv,
		Session: e.sessionID,
		Value:   []byte(e.sessionID),
	}
	res, _, err := e.Client.KV().Acquire(kv, nil)
	if err != nil {
		e.logError("Acquire kv error " + err.Error())
	}
	return res, err
}

func (e *Election) disableLeader() {
	if e.leader {
		e.leader = false
		e.logDebug("I'm not a leader.:(")
	}
}

func (e *Election) getKvSession() (string, error) {
	p, _, err := e.Client.KV().Get(e.Kv, nil)
	if err != nil {
		e.logError("Kv error " + err.Error())
		return "", err
	}
	if p == nil {
		return "", nil
	}
	return p.Session, nil
}

// Start election
func (e *Election) Init() {
	e.mutex.Lock()
	if e.inited {
		e.mutex.Unlock()
		e.logInfo("Only one init available")
		return
	}
	e.inited = true
	e.mutex.Unlock()
	for {
		if !e.inited {
			e.disableLeader()
			e.destroyCurrentSession()
			break
		}
		e.process()
		wait(e.CheckTimeout)
	}
}

func (e *Election) ReElection() error {
	s, err := e.getKvSession()
	if s != "" {
		e.destroySession(s)
	}
	return err
}

func (e *Election) destroySession(sesID string) error {
	_, err := e.Client.Session().Destroy(sesID, nil)
	if err != nil {
		e.logError("Destroy session error " + err.Error())
	}
	return err
}

func (e *Election) destroyCurrentSession() (err error) {
	if e.sessionID != "" {
		err = e.destroySession(e.sessionID)
		e.sessionID = ""
	}
	return
}

func (e *Election) isNeedAquire() bool {
	var res string
	var err error
	for {
		if !e.inited {
			break
		}
		res, err = e.getKvSession()
		if err != nil {
			e.disableLeader()
			wait(e.CheckTimeout)
		} else {
			break
		}

	}
	if e.sessionID != "" && e.sessionID == res {
		e.enableLeader()
	}
	if res == "" || res != e.sessionID {
		e.disableLeader()
	}

	return res == ""
}

func (e *Election) process() {
	e.waitSession()
	if !e.leader {
		if !e.isNeedAquire() {
			return
		}
		e.logDebug("Try to acquire")
		res, err := e.acquire()
		if res && err == nil {
			e.enableLeader()
		}
	}
}

func (e *Election) enableLeader() {
	if e.inited {
		e.leader = true
		e.logDebug("I'm a leader!")
	}

}

func (e *Election) Stop() {
	e.mutex.Lock()
	if e.inited {
		e.inited = false
		e.disableLeader()
	}
	e.mutex.Unlock()
}

func (e *Election) waitSession() {
	for {
		if !e.inited {
			break
		}
		isset, err := e.checkSession()

		if isset {
			e.logDebug("Session " + e.sessionID + " already exists")
			break
		}
		e.disableLeader()
		if err != nil {
			e.logDebug("Try to get session info again.")
			wait(e.CheckTimeout)
			continue
		}
		err = e.createSession()

		if err == nil {
			e.logDebug("Session " + e.sessionID + " created")
			break
		}
		wait(e.CheckTimeout)
	}
}

func wait(t time.Duration) {
	runtime.Gosched()
	time.Sleep(t)
}

func (e *Election) logError(err string) {
	if e.logLevel >= LogError {
		log.Println(e.LogPrefix + " [ERROR] " + err)
	}
}

func (e *Election) logDebug(s string) {
	if e.logLevel >= LogDebug {
		log.Println(e.LogPrefix + " [DEBUG] " + s)
	}
}

func (e *Election) logInfo(s string) {
	if e.logLevel >= LogInfo {
		log.Println(e.LogPrefix + " [INFO] " + s)
	}
}
