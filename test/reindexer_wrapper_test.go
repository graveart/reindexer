package reindexer

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/graveart/reindexer"
	_ "github.com/graveart/reindexer/bindings/cproto"
	// _ "github.com/graveart/reindexer/bindings/builtinserver"
)

type ReindexerWrapper struct {
	reindexer.Reindexer
	isMaster     bool
	slaveList    []*ReindexerWrapper
	master       *ReindexerWrapper
	dsn          string
	syncedStatus int32
	syncMutex    sync.RWMutex
}

func NewReindexWrapper(dsn string, options ...interface{}) *ReindexerWrapper {
	return &ReindexerWrapper{Reindexer: *reindexer.NewReindex(dsn, options...), isMaster: true, dsn: dsn, syncedStatus: 0}
}

func (dbw *ReindexerWrapper) setSynced() {
	atomic.StoreInt32(&dbw.syncedStatus, 1)
}

func (dbw *ReindexerWrapper) SetSyncRequired() {
	dbw.syncMutex.RLock()
	atomic.StoreInt32(&dbw.syncedStatus, 0)
	dbw.syncMutex.RUnlock()
}

func (dbw *ReindexerWrapper) IsSynced() bool {
	status := atomic.LoadInt32(&dbw.syncedStatus)
	if status == 0 {
		return false
	}
	return true
}

func (dbw *ReindexerWrapper) addSlave(dsn string, options ...interface{}) *ReindexerWrapper {
	if dbw.dsn == dsn {
		return nil
	}
	slaveDb := NewReindexWrapper(dsn, options...)
	slaveDb.isMaster = false
	slaveDb.master = dbw
	slaveDb.SetSyncRequired()
	dbw.slaveList = append(dbw.slaveList, slaveDb)
	dbw.setSlaveConfig(slaveDb)

	return slaveDb
}

func (dbw *ReindexerWrapper) AddSlave(dsn string, count int, options ...interface{}) []*ReindexerWrapper {
	u, err := url.Parse(dsn)
	if err != nil {
		panic(err)
	}
	for count > 0 {
		db := dbw.addSlave(dsn, options...)
		if db == nil {
			count++
		}
		us := u
		us.Path = us.Path + strconv.Itoa(count)
		dsn = us.String()
		count--
	}
	return dbw.slaveList
}

func (dbw *ReindexerWrapper) SetLogger(log reindexer.Logger) {
	dbw.Reindexer.SetLogger(log)
	for _, db := range dbw.slaveList {
		db.Reindexer.SetLogger(log)
	}
}

func (dbw *ReindexerWrapper) OpenNamespace(namespace string, opts *reindexer.NamespaceOptions, s interface{}) (err error) {
	dbw.SetSyncRequired()
	err = dbw.Reindexer.OpenNamespace(namespace, opts, s)
	if err != nil {
		return err
	}

	for _, db := range dbw.slaveList {
		db.RegisterNamespace(namespace, opts, s)
	}

	newTestNamespace(namespace, s)

	return err
}

func (dbw *ReindexerWrapper) DropNamespace(namespace string) error {
	dbw.SetSyncRequired()

	err := dbw.Reindexer.DropNamespace(namespace)
	if err != nil {
		return err
	}

	removeTestNamespce(namespace)
	return err
}

func (dbw *ReindexerWrapper) Query(namespace string) *queryTest {
	return newTestQuery(dbw, namespace)
}

func (dbw *ReindexerWrapper) GetBaseQuery(namespace string) *reindexer.Query {
	return dbw.Reindexer.Query(namespace)

}

func (dbw *ReindexerWrapper) execQuery(qt *queryTest) *reindexer.Iterator {
	return dbw.execQueryCtx(context.Background(), qt)
}

func (dbw *ReindexerWrapper) execQueryCtx(ctx context.Context, qt *queryTest) *reindexer.Iterator {
	if len(dbw.slaveList) == 0 || !qt.readOnly {
		if !qt.readOnly {
			dbw.SetSyncRequired()
		}
		return qt.q.ExecCtx(ctx)
	}
	if !qt.deepReplEqual {
		sdb := dbw.slaveList[rand.Intn(len(dbw.slaveList))]
		if !dbw.IsSynced() {
			dbw.syncMutex.Lock()
			sdb.WaitForSyncWithMaster()
			dbw.setSynced()
			sdb.ResetCaches()
			dbw.syncMutex.Unlock()
		}
		slaveQuery := qt.q.MakeCopy(&sdb.Reindexer)
		return slaveQuery.ExecCtx(ctx)
	}

	baseQuery := qt.q.MakeCopy(&dbw.Reindexer)
	rm, err := baseQuery.ExecCtx(ctx).FetchAll()
	if err != nil {
		return qt.q.MustExecCtx(ctx)
	}
	m := make(map[string]interface{})
	for _, item := range rm {
		pk := getPK(qt.ns, reflect.Indirect(reflect.ValueOf(item)))
		m[pk+reflect.TypeOf(item).String()] = item
	}

	if !dbw.IsSynced() {
		dbw.syncMutex.Lock()
		for _, db := range dbw.slaveList {
			db.WaitForSyncWithMaster()
		}
		dbw.setSynced()
		dbw.syncMutex.Unlock()
	}
	for _, db := range dbw.slaveList {
		slaveQuery := qt.q.MakeCopy(&db.Reindexer)
		rs, err := slaveQuery.ExecCtx(ctx).FetchAll()
		if err != nil {
			panic(err)
		}
		if len(rs) != len(rm) {
			panic(fmt.Errorf("Slave answer not equal to master"))
		}
		for _, item := range rs {
			pk := getPK(qt.ns, reflect.Indirect(reflect.ValueOf(item)))
			if mitem, ok := m[pk+reflect.TypeOf(item).String()]; ok {
				if !reflect.DeepEqual(mitem, item) {
					panic("Slave answer not equal to master")
				}
			} else {
				panic(fmt.Errorf("Slave answer not equal to master"))
			}
		}
		//TODO NOT GOOD - can be not equal do somthing
		//reflect.DeepEqual(rm, rs)
	}

	return qt.q.MustExecCtx(ctx)
}

func (dbw *ReindexerWrapper) setSlaveConfig(slaveDb *ReindexerWrapper) {
	err := dbw.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:        "replication",
		Replication: &reindexer.DBReplicationConfig{Namespaces: []string{}, Role: "master", ClusterID: 1, ForceSyncOnLogicError: true, ForceSyncOnWrongDataHash: true},
	})
	if err != nil {
		panic(err)
	}

	err = slaveDb.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:        "replication",
		Replication: &reindexer.DBReplicationConfig{Namespaces: []string{}, Role: "slave", MasterDSN: *dsn, ClusterID: 1, ForceSyncOnLogicError: true, ForceSyncOnWrongDataHash: true},
	})
	if err != nil {
		panic(err)
	}

}

func (dbw *ReindexerWrapper) WaitForSyncWithMaster() {
	complete := true

	var nameBad string
	var masterBadLsn reindexer.LsnT
	var slaveBadLsn reindexer.LsnT

	for i := 0; i < 600*5; i++ {

		complete = true

		stats, err := dbw.master.GetNamespacesMemStat()
		if err != nil {
			panic(err)
		}
		slaveStats, err := dbw.GetNamespacesMemStat()
		if err != nil {
			panic(err)
		}

		slaveLsnMap := make(map[string]reindexer.NamespaceMemStat)
		for _, st := range slaveStats {
			slaveLsnMap[st.Name] = *st
		}

		for _, st := range stats { // loop master namespaces stats

			if len(st.Name) == 0 || st.Name[0] == '#' {
				continue
			}

			if slaveLsn, ok := slaveLsnMap[st.Name]; ok {
				if slaveLsn.Replication.LastUpstreamLSN != st.Replication.LastLSN { //slave != master
					complete = false
					nameBad = st.Name
					masterBadLsn = st.Replication.LastLSN
					slaveBadLsn = slaveLsn.Replication.LastUpstreamLSN
					time.Sleep(100 * time.Millisecond)
					break
				}
			} else {
				complete = false
				nameBad = st.Name
				masterBadLsn.ServerId = 0
				masterBadLsn.Counter = 0
				slaveBadLsn.ServerId = 0
				slaveBadLsn.Counter = 0
				time.Sleep(100 * time.Millisecond)
				break
			}
		}
		if complete {
			for _, st := range stats {
				slaveLsn, _ := slaveLsnMap[st.Name]
				if slaveLsn.Replication.DataHash != st.Replication.DataHash {
					panic(fmt.Sprintf("Can't sync slave ns with master: ns \"%s\" slave dataHash: %d , master dataHash %d", st.Name, slaveLsn.Replication.DataHash, st.Replication.DataHash))
				}
			}
			dbw.setSynced()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	panic(fmt.Sprintf("Can't sync slave ns with master: ns \"%s\" masterlsn: %d , slavelsn %d", nameBad, masterBadLsn, slaveBadLsn))
}

func (dbw *ReindexerWrapper) TruncateNamespace(namespace string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.TruncateNamespace(namespace)
}

func (dbw *ReindexerWrapper) RenameNamespace(srcNsName string, dstNsName string) error {
	dbw.SetSyncRequired()

	err := dbw.Reindexer.RenameNamespace(srcNsName, dstNsName)
	if err != nil {
		return err
	}
	//change client ns tables
	for _, db := range dbw.slaveList {
		db.Reindexer.RenameNs(srcNsName, dstNsName)
	}

	renameTestNamespace(srcNsName, dstNsName)
	return err
}

func (dbw *ReindexerWrapper) CloseNamespace(namespace string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.CloseNamespace(namespace)
}

func (dbw *ReindexerWrapper) Upsert(namespace string, item interface{}, precepts ...string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.Upsert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) Insert(namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSyncRequired()
	return dbw.Reindexer.Insert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) Update(namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSyncRequired()
	return dbw.Reindexer.Update(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) Delete(namespace string, item interface{}, precepts ...string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.Delete(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) UpsertCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.WithContext(ctx).Upsert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) InsertCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSyncRequired()
	return dbw.Reindexer.WithContext(ctx).Insert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) UpdateCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSyncRequired()
	return dbw.Reindexer.WithContext(ctx).Update(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) DeleteCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.WithContext(ctx).Delete(namespace, item, precepts...)
}
