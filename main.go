package main

import (
	"flag"
	"fmt"

	"github.com/antongulenko/golib"
	log "github.com/sirupsen/logrus"
)

func main() {
	var frontends, backends golib.KeyValueStringSlice
	var restEndpoint string
	flag.Var(&frontends, "f", "One or more name=endpoint pairs defining frontend listening sockets")
	flag.Var(&backends, "b", "Optional initial name=endpoint pairs defining initially configured servers")
	flag.StringVar(&restEndpoint, "h", ":8080", "The HTTP listening socket for the REST API")
	golib.RegisterLogFlags()
	golib.RegisterTaskFlags()
	flag.Parse()
	golib.ConfigureLogging()
	if len(frontends.Keys) == 0 {
		log.Fatalln("Please define at least one frontend with -f")
	}

	tasks := new(golib.TaskGroup)
	balancer := NewTcpBalancer(frontends.Map(), tasks)
	tasks.Add(&golib.NoopTask{
		Description: fmt.Sprintf("Register %v predefined Backends", len(backends.Keys)),
		Chan: golib.WaitForSetup(nil, func() error {
			for i, name := range backends.Keys {
				if err := balancer.AddBackend(name, backends.Values[i]); err != nil {
					return err
				}
			}
			return nil
		}),
	})
	engine := golib.NewGinEngine()
	balancer.InitRestApi(engine)
	tasks.Add(golib.GinTask(engine, restEndpoint))
	tasks.Add(&golib.NoopTask{
		Chan:        golib.ExternalInterrupt(),
		Description: "external interrupt",
	})
	tasks.PrintWaitAndStop()
}
