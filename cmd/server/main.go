package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/c12s/kuiper/internal/configs"
	"github.com/c12s/kuiper/internal/startup"
)

func main() {
	config, err := configs.NewFromEnv()
	if err != nil {
		log.Fatalln(err)
	}

	app, err := startup.NewAppWithConfig(config)
	if err != nil {
		log.Fatalln(err)
	}
	err = app.Start()
	if err != nil {
		log.Fatalln(err)
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGTERM, syscall.SIGINT)

	<-shutdown

	app.GracefulStop()
}
