package taskrunner

import (
	"testing"
	"context"
	"time"
	"strconv"
	"sync"
	"github.com/stretchr/testify/assert"
)

func TestSampleTask(t *testing.T)  {
	ch := make(chan int, 5000)
	var wg sync.WaitGroup
	fn := func(ctx context.Context, mtr Task) {
		for{
			num := <-ch
			mtr.AddMeta(strconv.Itoa(num), nil)
			wg.Done()
			tc := time.After(time.Second*10)
			select {
			case <-ctx.Done():
				wg.Done()
				return
			case <-tc :
			}
		}
	}
	tm := NewTaskManager()
	for i:=0; i<20; i++ {
		wg.Add(1)
		tm.GO(fn)
	}
	for i:=0; i<100; i++{
		ch<-i
	}
	wg.Wait()
	wg.Add(1)
	tm.CancelTaskFromMetaKey("1")
	wg.Wait()
}

func TestAddMeta(t *testing.T)  {
	var wg sync.WaitGroup
	fn := func(ctx context.Context, task Task){
		task.AddMeta("addMeta", "sampleMeta")
		wg.Done()
		for {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			}
		}
	}
	tm := NewTaskManager()
	wg.Add(1)
	_, err := tm.GO(fn)
	assert.NoError(t, err)
	wg.Wait()
	tasks := tm.FindFromMetaKey("addMeta")
	assert.Len(t, tasks, 1)
	sampleTask := tasks[0]
	sampleTask.RemoveMeta("addMeta")
	tasks = tm.FindFromMetaKey("addMeta")
	assert.Len(t, tasks, 0)
	wg.Add(1)
	sampleTask.Cancel()
	wg.Wait()
}

func TestErrorCase(t *testing.T)  {
	var wg sync.WaitGroup
	fn := func(ctx context.Context, task Task){
		task.AddMeta("addMeta", "sampleMeta")
		wg.Done()
		for {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			}
		}
	}
	tm := NewTaskManager()
	wg.Add(1)
	_, err := tm.GO(fn)
	assert.NoError(t, err)
	wg.Wait()
	tasks := tm.FindFromMetaKey("addMeta")
	assert.Len(t, tasks, 1)
	sampleTask := tasks[0]
	err = sampleTask.AddMeta("addMeta", "Duplicate")
	assert.Error(t, err)
	assert.EqualError(t, err, ErrorKeyAlreadyExistInMeta.Error())

	err = sampleTask.RemoveMeta("addMeta")
	assert.NoError(t, err)
	err = sampleTask.RemoveMeta("addMeta")
	assert.Error(t, err)
	assert.EqualError(t, err, ErrorKeyDidNotExistInMeta.Error())
}