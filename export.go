package mongo4g

import (
	"gopkg.in/mgo.v2"
)

var mgr = &connectorManager{configs: &configRegistry{}}

func Connect(path string) *mgo.Database {
	return mgr.Connect(path)
}

func Close(database *mgo.Database) {
	if database != nil {
		database.Session.Close()
	}
}

func GetConf(path string) (cp ConnectionParameters) {
	if conf := mgr.configs.GetConf(path); conf != nil {
		cp = *conf
	}
	return
}
