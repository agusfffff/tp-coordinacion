package shutdown

import (
	"context"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func StopConsumingOnSignal(consumer middleware.Middleware) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-ctx.Done()
		stop()
		slog.Info("recieved SIGTERM")
		if err := consumer.StopConsuming(); err != nil {
			slog.Error("While stopping consumer", "err", err)
		}
	}()
}
