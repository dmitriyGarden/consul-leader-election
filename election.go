package election

// Leader election
// https://www.consul.io/docs/guides/leader-election.html
import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)

// Log levels
const (
	LogDisable = iota
	LogError
	LogInfo
	LogDebug
)

// Election implements to detect a leader in a cluster of services
type Election struct {
	Client           *api.Client // Consul client
	Checks           []string    // Slice of associated health checks
	leader           bool        // Flag of a leader
	Kv               string      // Key in Consul kv
	sessionID        string      // Id of session
	logLevel         uint8       //  Log level LogDisable|LogError|LogInfo|LogDebug
	inited           bool        // Flag of init.
	CheckTimeout     time.Duration
	SessionLockDelay time.Duration
	LogPrefix        string // Prefix for a log
	ctx              context.Context
	done             context.CancelFunc
	success          chan struct{}
	Event            Notifier
	sync.RWMutex
}

// Notifier can tell your code the event of the leader's status change
type Notifier interface {
	EventLeader(e bool) // The method will be called when the leader status is changed
}

// ElectionConfig config for Election
type ElectionConfig struct {
	Client           *api.Client // Consul client
	Checks           []string    // Slice of associated health checks
	Key              string      // Key in Consul KV
	LogLevel         uint8       // Log level LogDisable|LogError|LogInfo|LogDebug
	LogPrefix        string      // Prefix for a log
	Event            Notifier
	CheckTimeout     time.Duration
	SessionLockDelay time.Duration
}

// IsLeader check a leader
func (e *Election) IsLeader() bool {
	e.RLock()
	defer e.RUnlock()
	return e.leader
}

// SetLogLevel is setting level according constants LogDisable|LogError|LogInfo|LogDebug
func (e *Election) SetLogLevel(level uint8) {
	e.logLevel = level
}

// NewElection create new elector
func NewElection(c *ElectionConfig) *Election {
	ctx, done := context.WithCancel(context.Background())
	e := &Election{
		Client:           c.Client,
		Checks:           append(c.Checks, "serfHealth"),
		leader:           false,
		Kv:               c.Key,
		CheckTimeout:     c.CheckTimeout,
		SessionLockDelay: c.SessionLockDelay,
		LogPrefix:        c.LogPrefix,
		ctx:              ctx,
		done:             done,
		success:          make(chan struct{}),
		Event:            c.Event,
	}
	return e
}

func (e *Election) createSession() (err error) {
	ses := &api.SessionEntry{
		Checks:    e.Checks,
		TTL:       (3 * e.CheckTimeout).String(),
		LockDelay: e.SessionLockDelay,
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
	e.Lock()
	if e.leader {
		e.leader = false
		e.logDebug("I'm not a leader.:(")
		if e.Event != nil {
			e.Event.EventLeader(false)
		}
	}
	e.Unlock()
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

// Init starting election process
func (e *Election) Init() {
	e.InitContext(context.Background())
}

// InitContext starting election process with context
func (e *Election) InitContext(ctx context.Context) {
	e.Lock()
	if e.inited {
		e.Unlock()
		e.logInfo("Only one init available")
		return
	}
	e.inited = true
	e.done()
	e.ctx, e.done = context.WithCancel(ctx)
	e.Unlock()
	e.background()
	e.logDebug("I'm finished")
}

func (e *Election) background() {
	e.process()
	ticker := time.NewTicker(e.CheckTimeout)
	for {
		select {
		case <-ticker.C:
			e.process()
		case <-e.ctx.Done():
			ticker.Stop()
			e.inited = false
			e.logDebug("Stop signal received")
			e.disableLeader()
			e.destroyCurrentSession()
			e.success <- struct{}{}
			e.logDebug("Send success")
			return
		}
	}
}

//ReElection start re-election
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

func (e *Election) waitSessionData() (string, error) {
	res, err := e.getKvSession()
	if err == nil {
		return res, nil
	}
	e.disableLeader()
	ticker := time.NewTicker(e.CheckTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			res, err = e.getKvSession()
			if err == nil {
				return res, nil
			}
		case <-e.ctx.Done():
			return "", errors.New("cancelled")
		}
	}
}

func (e *Election) isNeedAquire() bool {
	res, err := e.waitSessionData()
	if err != nil {
		return false
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
	e.Lock()
	e.leader = true
	e.logDebug("I'm a leader!")
	if e.Event != nil {
		e.Event.EventLeader(true)
	}
	e.Unlock()
}

// Stop election process
func (e *Election) Stop() {
	e.RLock()
	if !e.inited {
		e.RUnlock()
		return
	}
	e.RUnlock()
	e.done()
	<-e.success
}

func (e *Election) processSession() error {
	isset, err := e.checkSession()

	if isset {
		e.Client.Session().Renew(e.sessionID, nil)
		return nil
	}
	e.disableLeader()
	if err != nil {
		e.logDebug("Try to get session info again.")
		return err
	}
	err = e.createSession()

	if err == nil {
		e.logDebug("Session " + e.sessionID + " created")
		return err
	}
	return nil
}

func (e *Election) waitSession() {
	err := e.processSession()
	if err == nil {
		return
	}
	ticker := time.NewTicker(e.CheckTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err = e.processSession()
			if err == nil {
				return
			}
		case <-e.ctx.Done():
			return
		}
	}
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
