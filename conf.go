package mongo4g

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

import (
	"github.com/xgo11/configuration"
	"github.com/xgo11/datetime"
	"github.com/xgo11/env"
)

//connect_string: "mongodb://localhost/result"
//# 连接池大小，控制最大并发，最大4096
//pool_limit: 1000

const (
	prefixPath   = "db/mongo"
	maxPoolLimit = 4096
)

type ConnectionParameters struct {
	ConnectString string `yaml:"connect_string" json:"connect_string"`
	PoolLimit     int    `yaml:"pool_limit" json:"pool_limit"`

	path     string // config file path
	database string // database name
	file     string // config file name
	lstmod   int64  // last load time
}

func fulfillPath(path string) string {
	path = strings.Trim(path, "/")
	if strings.HasPrefix(path, prefixPath) {
		return path
	}
	return prefixPath + "/" + path
}

func NewConnectionParams(path string) (cp ConnectionParameters, err error) {
	path = fulfillPath(path)
	cp.path = path
	cp.file = filepath.Join(env.ConfDir(), path+".yaml")

	if err = configuration.LoadRelativePath(path, &cp); err != nil {
		return
	}

	if p1 := strings.LastIndex(cp.ConnectString, "/"); p1 > 0 {
		if p2 := strings.LastIndex(cp.ConnectString, "?"); p2 > p1 {
			cp.database = cp.ConnectString[p1+1 : p2]
		} else {
			cp.database = cp.ConnectString[p1+1:]
		}
	}
	if cp.database == "" {
		err = fmt.Errorf("database name empty")
		return
	}
	var info os.FileInfo
	if info, err = os.Stat(cp.file); err != nil {
		return
	}
	cp.lstmod = info.ModTime().In(datetime.LocalTZ()).Unix()
	if cp.PoolLimit > maxPoolLimit {
		cp.PoolLimit = maxPoolLimit
	} else if cp.PoolLimit <= 0 {
		cp.PoolLimit = 1000
	}
	return
}

func (cp *ConnectionParameters) Database() string {
	return cp.database
}
func (cp *ConnectionParameters) Path() string {
	return cp.path
}

func (cp *ConnectionParameters) File() string {
	return cp.file
}

func (cp *ConnectionParameters) LstMod() int64 {
	return cp.lstmod
}

func (cp ConnectionParameters) String() string {
	return fmt.Sprintf("<%s> %s@%d", cp.Path(), cp.Database(), cp.LstMod())
}

func (cp *ConnectionParameters) JSON(indent ...int) string {
	var tab int
	if len(indent) > 0 {
		tab = indent[0]
	}
	if tab < 0 {
		tab = 0
	}
	if tab == 0 {
		bs, _ := json.Marshal(cp)
		return string(bs)
	} else {
		bs, _ := json.MarshalIndent(cp, "", strings.Repeat(" ", tab))
		return string(bs)
	}

}
