package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// ЗАДАНИЕ:
// * сделать из плохого кода хороший;
// * важно сохранить логику появления ошибочных тасков;
// * сделать правильную мультипоточность обработки заданий.
// Обновленный код отправить через merge-request.

// приложение эмулирует получение и обработку тасков, пытается и получать и обрабатывать в многопоточном режиме
// В конце должно выводить успешные таски и ошибки выполнения остальных тасков

const (
	// длительность генерации задач
	generatorTimeout = 1 * time.Second
)

var (
	// обрабатывать не более задач за раз
	maxTaskWorkers = runtime.NumCPU()
)

// A Ttype represents a meaninglessness of our life
type Task struct {
	id          int64
	createdAt   time.Time // время создания
	processedAt time.Time // время выполнения
	taskResult  string
}

func taskCreator(ctx context.Context, resChan chan<- Task) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ct := time.Now()
			// генерируем "устаревшие" задачи
			if ct.Second()%3 == 0 {
				ct = ct.Add(-30 * time.Second)
			}
			if ct.Nanosecond()%2 > 0 { // вот такое условие появления ошибочных тасков (более показательно изменить делитель на >2)
				ct = time.Time{} // нулевое время как признак ошибки
			}
			// UnixTime ID плохой выбор - малогранулядно, будут пересечения хороших/плохих и дубли - лучше взять crypto/rand или хотя бы милли/наносекунды
			resChan <- Task{createdAt: ct, id: time.Now().UnixMilli()} // передаем таск на выполнение
		}
	}
}

func taskWorker(t Task) (Task, error) {
	var err error

	// слишком старые или ошибочные (нулевое время) таски обрабатывать смысла нет
	switch {
	case t.createdAt == time.Time{}: // "ошибки" генерации
		err = fmt.Errorf("something went wrong")
	case time.Since(t.createdAt) > 20*time.Second: // "залипшие"
		err = fmt.Errorf("stale task")
	default:
		t.taskResult = "task has been successed"
	}
	t.processedAt = time.Now()

	time.Sleep(150 * time.Millisecond)

	return t, err
}

func main() {
	var (
		taskChan = make(chan Task, 10)
		// группа сервисный функций
		wg            errgroup.Group
		done          = make(map[int64]Task)
		errored       = make([]error, 0, 10)
		muDone, muErr sync.Mutex
	)

	// сколько воркеров запускать + 1 генератор
	wg.SetLimit(maxTaskWorkers + 1)

	// создание тасков
	// задаём время работы генератора тасков
	ctx, cancel := context.WithTimeout(context.Background(), generatorTimeout)
	defer cancel()
	wg.Go(func() error {
		taskCreator(ctx, taskChan)
		// сообщаем обработчикам, что работы больше нет
		close(taskChan)
		return nil
	})

	// обработка тасков
	for t := range taskChan {
		t := t
		wg.Go(func() error {
			res, err := taskWorker(t)
			// Можно писать в каналы или сразу заполнять нужные слайсы - всё одно мьютексы, явные или неявные, но канал сильно медленнее
			if err != nil {
				// заполнять результат внутри задачи смысла нет - он не используется
				// t.taskResult = err.Error()
				muErr.Lock()
				errored = append(errored, fmt.Errorf("task id %d created at %q, error is %q", res.id, res.createdAt, err))
				muErr.Unlock()
			} else {
				muDone.Lock()
				done[res.id] = res
				muDone.Unlock()
			}
			return nil
		})
	}

	// ждём завершения горутин, ошибок мы не генерим, потому игнорим
	_ = wg.Wait()

	// вывод результатов
	fmt.Println("Errors:")
	for _, e := range errored {
		fmt.Println(e)
	}

	fmt.Println("Done tasks:")
	for r := range done {
		fmt.Println(r)
	}
}
