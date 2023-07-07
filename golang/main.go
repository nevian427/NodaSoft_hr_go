package main

import (
	"context"
	"fmt"
	"runtime"
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
	if time.Since(t.createdAt) < 20*time.Second {
		t.taskResult = "task has been successed"
	} else {
		// "ошибки" генерации
		if (t.createdAt == time.Time{}) {
			t.taskResult = "something went wrong"
		} else {
			// "залипшие"
			t.taskResult = "stale task"
		}
		err = fmt.Errorf("task id %d created at %s, error is \"%s\"", t.id, t.createdAt, t.taskResult)
	}
	t.processedAt = time.Now()

	time.Sleep(150 * time.Millisecond)

	return t, err
}

func main() {
	taskChan := make(chan Task, 10)
	doneTasks := make(chan Task)
	undoneTasks := make(chan error)
	// группа сервисный функций
	serv_wg := errgroup.Group{}
	// группа обработчиков
	task_wg := errgroup.Group{}
	// сколько воркеров запускать
	task_wg.SetLimit(maxTaskWorkers)
	result := map[int64]Task{}
	err := make([]error, 0, 10)

	// задаём время работы генератора тасков
	ctx, cancel := context.WithTimeout(context.Background(), generatorTimeout)
	defer cancel()

	// создание тасков
	serv_wg.Go(func() error {
		taskCreator(ctx, taskChan)
		// сообщаем обработчикам, что работы больше нет
		close(taskChan)
		return nil
	})

	// получение результатов
	serv_wg.Go(func() error {
		for {
			select {
			case r, ok := <-doneTasks:
				if ok {
					result[r.id] = r
				} else {
					// канал закрыт - исключаем его
					doneTasks = nil
				}
			case r, ok := <-undoneTasks:
				if ok {
					err = append(err, r)
				} else {
					// канал закрыт - исключаем его
					undoneTasks = nil
				}
			}
			// все результаты собраны
			if undoneTasks == nil && doneTasks == nil {
				break
			}
		}
		return nil
	})

	// обработка тасков
	for t := range taskChan {
		t := t
		task_wg.Go(func() error {
			res, err := taskWorker(t)
			// Можно писать в каналы или сразу заполнять нужные слайсы - всё одно мьютексы, явные или неявные
			if err != nil {
				undoneTasks <- err
			} else {
				doneTasks <- res
			}
			return nil
		})
	}
	// ждём завершения обработчиков
	task_wg.Wait()

	// завершаем сбор результатов
	close(undoneTasks)
	close(doneTasks)

	// ждём окончания сбора результатов
	serv_wg.Wait()

	// вывод результатов
	fmt.Println("Errors:")
	for _, e := range err {
		fmt.Println(e)
	}

	fmt.Println("Done tasks:")
	for r := range result {
		fmt.Println(r)
	}
}
