package mongo4g

import (
	"sync"
	"time"
)

import (
	"github.com/xgo11/stdlog"
	"gopkg.in/mgo.v2"
)

var log = stdlog.Std

type connection struct {
	sync.Mutex

	conf    *ConnectionParameters
	session *mgo.Session
}

type configRegistry struct {
	sync.Mutex

	registry map[string]*ConnectionParameters
}

type connectorManager struct {
	sync.Mutex

	configs     *configRegistry
	connections map[string]*connection
}

func (r *configRegistry) GetConf(path string) *ConnectionParameters {
	path = fulfillPath(path)

	r.Lock()
	defer r.Unlock()

	if r.registry == nil {
		r.registry = make(map[string]*ConnectionParameters)
	}

	if cp, ok := r.registry[path]; ok {
		return cp
	}

	if cp, err := NewConnectionParams(path); err != nil {
		log.Errorf("Load connection config fail, path=%s, err=%v", path, err)
		return nil
	} else {
		r.registry[cp.Path()] = &cp
		log.Debugf("Load connection config ok, %v", cp.String())
		return &cp
	}
}

func (m *connectorManager) Connect(path string) *mgo.Database {

	conf := m.configs.GetConf(path)
	if conf == nil { // no configuration
		return nil
	}
	m.Lock()
	if m.connections == nil {
		m.connections = make(map[string]*connection)
	}
	var c = m.connections[conf.Path()]
	if c == nil {
		c = &connection{conf: conf}
		m.connections[conf.Path()] = c
	}
	m.Unlock()

	return c.Connect()
}

func (c *connection) Connect() *mgo.Database {
	c.Lock()
	defer c.Unlock()

	var connectErr error
	var sCopy *mgo.Session

	if c.session != nil {
		sCopy = c.session.Clone()
		if connectErr = sCopy.Ping(); connectErr == nil {
			return sCopy.DB(c.conf.Database())
		}
		sCopy.Close()
		c.session.Close()
		log.Warnf("%v, ping fail, %v", c.conf, connectErr)
	}

	if c.session, connectErr = mgo.DialWithTimeout(c.conf.ConnectString, 3*time.Second); connectErr != nil {
		log.Errorf("%v, dail fail, %v", c.conf, connectErr)
		return nil
	}

	if connectErr = c.session.Ping(); connectErr != nil {
		log.Errorf("%v, ping fail, %v", c.conf, connectErr)
		return nil
	}
	log.Debugf("mongo session opened, %v", c.conf)
	sCopy = c.session.Clone()
	return sCopy.DB(c.conf.Database())
}
