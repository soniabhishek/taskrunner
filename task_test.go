package taskrunner

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSampleTask(t *testing.T) {
	ch := make(chan int, 5000)
	var wg sync.WaitGroup
	fn := func(ctx context.Context, mtr Task) {
		for {
			num := <-ch
			mtr.AddMeta(strconv.Itoa(num), nil)
			wg.Done()
			tc := time.After(time.Second * 10)
			select {
			case <-ctx.Done():
				wg.Done()
				return
			case <-tc:
			}
		}
	}
	tm := NewTaskManager()
	ctx := context.TODO()
	for i := 0; i < 20; i++ {
		wg.Add(1)
		tm.GO(ctx, fn)
	}
	for i := 0; i < 100; i++ {
		ch <- i
	}
	wg.Wait()

	//check cancelling a task
	wg.Add(1)
	cnt, err := tm.CancelTaskFromMetaKey("1")
	assert.NoError(t, err)
	assert.Equal(t, 1, cnt)
	wg.Wait()
	cnt = tm.GetTasksCount()
	assert.Equal(t, 19, cnt)

	//check restarting a task
	wg.Add(2)
	cnt, err = tm.RestartTasksFromMetaKey("2")
	assert.NoError(t, err)
	assert.Equal(t, 1, cnt)
	wg.Wait()
	cnt = tm.GetTasksCount()
	assert.Equal(t, 19, cnt)
}

func TestAddMeta(t *testing.T) {
	var wg sync.WaitGroup
	fn := func(ctx context.Context, task Task) {
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
	ctx := context.TODO()
	_, err := tm.GO(ctx, fn)
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

func TestErrorCase(t *testing.T) {
	var wg sync.WaitGroup
	fn := func(ctx context.Context, task Task) {
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
	ctx := context.TODO()
	_, err := tm.GO(ctx, fn)
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
