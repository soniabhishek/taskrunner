package taskrunner

import (
	"sync"
	"context"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

var (
	ErrorKeyAlreadyExistInMeta = errors.New("key already exist in meta")
	ErrorKeyDidNotExistInMeta = errors.New("key is not present in meta")
	ErrorTaskManagerIsHaulted = errors.New("task manager is shutting down")
)

type runnable func(ctx context.Context, meta *Task)


type Task struct {
	sync.RWMutex
	cancelFunc context.CancelFunc
	source runnable
	meta map[string]interface{}
}

func (m *Task) AddMeta(key string, val interface{}) error{
	m.Lock()
	defer m.Unlock()
	if _, ok :=m.meta[key]; ok {
		return ErrorKeyAlreadyExistInMeta
	}
	m.meta[key] = val

	return nil
}

func (m *Task) RemoveMeta(key string) error{
	m.Lock()
	defer m.Unlock()
	if _, ok :=m.meta[key]; !ok {
		return ErrorKeyDidNotExistInMeta
	}
	delete(m.meta, key)
	return nil
}

func (m *Task) Cancel(){
	m.Lock()
	defer m.Unlock()
	m.cancelFunc()
}

type TaskManager struct {
	sync.RWMutex
	count int
	open bool
	ledger map[string]*Task
}

func (tm *TaskManager) addTask(cancelFunc context.CancelFunc, taskid string, fn runnable) *Task {
	tm.Lock()
	defer tm.Unlock()
	tm.count++
	tm.ledger[taskid] = newMetric(cancelFunc, fn)
	return tm.ledger[taskid]
}

func (tm *TaskManager) done(taskid string) {
	tm.Lock()
	defer tm.Unlock()
	tm.count--
	delete(tm.ledger, taskid)
}

func (tm *TaskManager) GO(fn runnable) (taskId string, err error) {
	if tm.IsOpen() {
		taskId = uuid.NewV4().String()
		go func() {
			ctx,cancel := context.WithCancel(context.Background())
			fn(ctx, tm.addTask(cancel, taskId, fn))
			tm.done(taskId)
		}()
		return
	}
	return taskId, ErrorTaskManagerIsHaulted
}

func (tm *TaskManager) IsOpen() bool {
	tm.RLock()
	defer tm.RUnlock()
	return tm.open
}

func (tm *TaskManager) FindFromMeta(key string) (tasks []*Task) {
	tm.RLock()
	defer tm.RUnlock()

	for lkey, led := range tm.ledger{
		led.RLock()
		if _, ok :=led.meta[key]; ok {
			tasks = append(tasks, tm.ledger[lkey])
		}
		led.RUnlock()
	}
	return tasks
}


func (tm *TaskManager) CancelTaskFromMetaKey(key string) {
	tm.Lock()
	defer tm.Unlock()

	for lkey, led := range tm.ledger {
		led.RLock()
		if _, ok :=led.meta[key]; ok {
			tm.ledger[lkey].cancelFunc()
		}
		led.RUnlock()
	}
}


func (tm *TaskManager) RestartTasksFromMetaKey(key string) {
	tm.Lock()
	defer tm.Unlock()

	for lkey, led := range tm.ledger{
		led.RLock()
		if _, ok :=led.meta[key]; ok {
			task := tm.ledger[lkey]
			source := task.source
			task.cancelFunc()
			tm.GO(source)
		}
		led.RUnlock()
	}
}

