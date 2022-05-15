package dataflow

import (
    "context"
    "log"
    "os"
    "os/signal"
    "strings"
    "sync"
    "syscall"
    "time"
)

const (
    DEFAULT_WORKERS  = 2000
    DEFAULT_CHAN_LEN = 1000
    INPUT_STAGE      = "input"
    SOURCE_STAGE     = "source"
    OUTPUT_STAGE     = "output"
)

type CallbackType func(ctx context.Context, rch chan interface{}, wch chan interface{}, wg *sync.WaitGroup)

type CancelFunc func()

type Stage struct {
    StageType  string
    Stages     []*Stage
    ApplyFunc  CallbackType
    Wg         sync.WaitGroup
    Cancel     context.CancelFunc
    Ctx        context.Context
    ReaderChan chan interface{}
    WriteChan  chan interface{}
    Workers    int
    ChanCap    int
    ChanOn     bool
    test       int
}

var DataFlow Stage

func init() {
    DataFlow.WriteChan = make(chan interface{}, 1000)
    DataFlow.Ctx, DataFlow.Cancel = context.WithCancel(context.Background())
}

func (s *Stage) Input(Callback CallbackType, Workers int, ChanLen int) *Stage {
    e := new(Stage)
    e.StageType = INPUT_STAGE
    e.Workers = DEFAULT_WORKERS
    if Workers > 0 {
        e.Workers = Workers
    }
    e.ApplyFunc = Callback
    s.Stages = append(s.Stages, e)
    return e
}

func (s *Stage) Register(Callback CallbackType, Workers int, ChanLen int) *Stage {
    e := new(Stage)
    e.Workers = DEFAULT_WORKERS
    if Workers > 0 {
        e.Workers = Workers
    }
    if ChanLen > 0 {
        e.WriteChan = make(chan interface{}, ChanLen)
    } else {
        e.WriteChan = make(chan interface{}, DEFAULT_CHAN_LEN)
    }
    e.ApplyFunc = Callback
    e.ChanOn = true
    s.Stages = append(s.Stages, e)
    return e
}

func (s *Stage) Run() {
    if len(s.Stages) > 0 {
        lastStage := s
        for _, Stage := range s.Stages {
            if Stage.StageType == INPUT_STAGE {
                for i := 0; i < Stage.Workers; i++ {
                    s.Wg.Add(1)
                    go Stage.ApplyFunc(s.Ctx, s.ReaderChan, s.WriteChan, &s.Wg)
                }
                continue
            }
            for i := 0; i < Stage.Workers; i++ {
                s.Wg.Add(1)
                go Stage.ApplyFunc(s.Ctx, lastStage.WriteChan, Stage.WriteChan, &s.Wg)
            }
            lastStage = Stage
        }
    }
    if strings.Contains(os.Args[0], "Test") {
        log.Println("Stage Run End")
        time.Sleep(20 * time.Second)
        s.Stop()
        log.Println("Stage Run Wait after")
    }
}

func (s *Stage) Listen() {
    s.Wg.Add(1)
    go func() {
        defer s.Wg.Done()
        c := make(chan os.Signal)
        signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
        for si := range c {
            switch si {
            case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
                log.Println("\nService Stoping By signal:", si)
                s.Stop()
                return
            case syscall.SIGUSR1:
                log.Println("usr1", s)
            case syscall.SIGUSR2:
                log.Println("usr2", s)
            default:
                log.Println("other", s)
            }
        }
    }()
    s.Wg.Wait()
}

func (s *Stage) Stop() {
    log.Println("Stop")
    s.Cancel()
    time.Sleep(3 * time.Second)
    s.CloseChannel(s.WriteChan)
    if len(s.Stages) > 0 {
        for idx, Stage := range s.Stages {
            log.Println("stage", idx, Stage.StageType)
            if Stage.ChanOn == true {
                s.CloseChannel(Stage.WriteChan)
            }
        }
    }
    log.Println("Clear end")
    return
}

func (s *Stage) CloseChannel(c chan interface{}) {
    for {
        log.Println("CloseChannel ch len:", len(c))
        if len(c) == 0 {
            close(c)
            return
        }
        time.Sleep(1 * time.Second)
    }
}
