package log

import (
	"os"

	"github.com/lens077/go-connect-kit/config"
	"github.com/lens077/go-connect-kit/meta"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/log/global"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

const (
	FormatConsole = "console"
	FormatJSON    = "json"
)

// Options is the provider-neutral logging configuration consumed by this package.
type Options struct {
	Level               string
	Format              string
	FrameworkLogLevel   string
	FrameworkErrorLevel string
	StacktraceLevel     string
}

// Projector maps a caller-owned configuration message into logging Options.
type Projector[T proto.Message] func(T) Options

// Module builds an application logger and keeps its level synchronized with Live config.
func Module[T proto.Message](project Projector[T]) fx.Option {
	return fx.Module("log",
		fx.Provide(func(conf T, info meta.AppInfo, live *config.Live[T]) *zap.Logger {
			logger, level := newLogger(project(conf), info)
			live.Subscribe(func(_, cur T) {
				want := parseLevel(project(cur).Level)
				if want == level.Level() {
					return
				}
				level.SetLevel(want)
				logger.Info("log level changed", zap.String("level", want.String()))
			})
			return logger
		}),
	)
}

// FxLogger configures Fx's own event logger from the caller-owned configuration.
func FxLogger[T proto.Message](project Projector[T]) fx.Option {
	return fx.WithLogger(func(logger *zap.Logger, conf T) fxevent.Logger {
		options := project(conf)
		fxLogger := &fxevent.ZapLogger{Logger: logger}

		var logLevel zapcore.Level
		if err := logLevel.UnmarshalText([]byte(options.FrameworkLogLevel)); err != nil {
			logLevel = zapcore.DebugLevel
		}
		var errorLevel zapcore.Level
		if err := errorLevel.UnmarshalText([]byte(options.FrameworkErrorLevel)); err != nil {
			errorLevel = zapcore.ErrorLevel
		}

		fxLogger.UseLogLevel(logLevel)
		fxLogger.UseErrorLevel(errorLevel)
		return fxLogger
	})
}

// NewLogger constructs an application logger from provider-neutral options.
func NewLogger(options Options, info meta.AppInfo) *zap.Logger {
	logger, _ := newLogger(options, info)
	return logger
}

func parseLevel(value string) zapcore.Level {
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(value)); err != nil {
		return zapcore.DebugLevel
	}
	return level
}

func newLogger(options Options, info meta.AppInfo) (*zap.Logger, zap.AtomicLevel) {
	level := zap.NewAtomicLevelAt(parseLevel(options.Level))

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	var encoder zapcore.Encoder
	if options.Format == FormatConsole {
		encoderConfig = zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	}

	stdoutCore := zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level)
	otelCore := otelzap.NewCore(
		info.Name,
		otelzap.WithLoggerProvider(global.GetLoggerProvider()),
	)
	zapOptions := []zap.Option{zap.AddCaller()}
	if options.StacktraceLevel != "" {
		zapOptions = append(zapOptions, zap.AddStacktrace(parseLevel(options.StacktraceLevel)))
	}
	return zap.New(zapcore.NewTee(stdoutCore, otelCore), zapOptions...), level
}
