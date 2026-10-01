package shutdown

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func StopConsumingOnSignal(consumer middleware.Middleware) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signals
		slog.Info("recieved SIGTERM")
		if err := consumer.StopConsuming(); err != nil {
			slog.Error("Stopping consumer", "err", err)
		}
	}()
}
