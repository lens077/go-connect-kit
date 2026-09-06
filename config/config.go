package config

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"buf.build/go/protovalidate"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// LoadOptions controls decoding policy without coupling the module to a caller's schema.
type LoadOptions struct {
	AllowUnknownFields bool
	SkipValidation     bool
}

// SourceFactory builds the repository-owned Source adapter used by an Fx application.
type SourceFactory func() (Source, error)

// RestartRequiredSection identifies a section whose dependent resources are not reconfigured live.
type RestartRequiredSection struct {
	Name    string
	Message proto.Message
}

// RestartRequiredProjector extracts restart-only sections from a caller-owned Bootstrap.
type RestartRequiredProjector[T proto.Message] func(T) []RestartRequiredSection

// Module wires one concrete protobuf Bootstrap type and Source adapter into Fx.
func Module[T proto.Message](newSource SourceFactory, options LoadOptions, restartRequired RestartRequiredProjector[T]) fx.Option {
	return fx.Module("config",
		fx.Provide(
			func(lc fx.Lifecycle) (*Live[T], error) {
				if newSource == nil {
					return nil, fmt.Errorf("config source factory is nil")
				}
				ctx, cancel := context.WithCancel(context.Background())
				lc.Append(fx.Hook{OnStop: func(context.Context) error {
					cancel()
					return nil
				}})

				source, err := newSource()
				if err != nil {
					return nil, err
				}
				return NewWithOptions[T](ctx, source, options)
			},
			func(live *Live[T]) T { return live.Get() },
		),
		fx.Invoke(func(lc fx.Lifecycle, logger *zap.Logger, live *Live[T]) {
			startWatch(lc, logger, live, restartRequired)
		}),
	)
}

// New loads, decodes, and validates one configuration value from src.
func New[T proto.Message](src Source) (*Live[T], error) {
	return NewWithOptions[T](context.Background(), src, LoadOptions{})
}

// NewWithContext is New with an explicit load context.
func NewWithContext[T proto.Message](ctx context.Context, src Source) (*Live[T], error) {
	return NewWithOptions[T](ctx, src, LoadOptions{})
}

// NewWithOptions loads one configuration value using an explicit decoding policy.
func NewWithOptions[T proto.Message](ctx context.Context, src Source, options LoadOptions) (*Live[T], error) {
	if src == nil {
		return nil, fmt.Errorf("config source is nil")
	}

	rawConfig, err := src.Load(ctx)
	if err != nil {
		return nil, err
	}

	localConfig := newMessage[T]()
	if err := decodeConfig(rawConfig, localConfig, options); err != nil {
		return nil, fmt.Errorf("decode config from %s: %w", src.Name(), err)
	}
	if !options.SkipValidation {
		if err := validateMessage(localConfig); err != nil {
			return nil, fmt.Errorf("validate config from %s: %w", src.Name(), err)
		}
	}

	live := NewLive(localConfig)
	live.source = src
	live.decode = func(data map[string]any, target T) error {
		return decodeConfig(data, target, options)
	}
	if options.SkipValidation {
		live.validate = func(T) error { return nil }
	}
	return live, nil
}

func decodeConfig[T proto.Message](data map[string]any, target T, options LoadOptions) error {
	v := viper.New()
	v.SetConfigType("yaml")
	for key, value := range data {
		v.Set(key, value)
	}

	stringToProtoDurationHook := func(from reflect.Type, to reflect.Type, data any) (any, error) {
		if from.Kind() != reflect.String || to != reflect.TypeOf(&durationpb.Duration{}) {
			return data, nil
		}
		duration, err := time.ParseDuration(data.(string))
		if err != nil {
			return nil, err
		}
		return durationpb.New(duration), nil
	}

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName:     "json",
		ErrorUnused: !options.AllowUnknownFields,
		DecodeHook:  mapstructure.ComposeDecodeHookFunc(stringToProtoDurationHook),
		Result:      target,
	})
	if err != nil {
		return err
	}
	return decoder.Decode(v.AllSettings())
}

func validateMessage[T proto.Message](message T) error {
	return protovalidate.Validate(message)
}

func startWatch[T proto.Message](
	lc fx.Lifecycle,
	logger *zap.Logger,
	live *Live[T],
	restartRequired RestartRequiredProjector[T],
) {
	log := logger.Named("configWatch")

	watcher, ok := live.source.(Watcher)
	if !ok {
		log.Info("current config source does not support updates; config is loaded once",
			zap.String("source", live.SourceName()))
		return
	}

	if restartRequired != nil {
		live.Subscribe(func(old, cur T) {
			warnNotHotReloadable(log, restartRequired(old), restartRequired(cur))
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				err := watcher.Watch(ctx, func(event WatchEvent) {
					switch {
					case event.Err != nil:
						log.Error("config update cannot be processed; keeping current config", zap.Error(event.Err))
					case event.Deleted:
						log.Error("config was deleted at source; keeping current config",
							zap.String("source", live.SourceName()))
					default:
						cur := newMessage[T]()
						if err := live.decode(event.Raw, cur); err != nil {
							log.Error("config update decode failed; keeping current config", zap.Error(err))
							return
						}
						if err := live.validate(cur); err != nil {
							log.Error("config update validation failed; keeping current config", zap.Error(err))
							return
						}
						live.Set(cur)
						log.Info("config updated", zap.String("source", live.SourceName()))
					}
				})
				if err != nil && ctx.Err() == nil {
					log.Error("config watch stopped; keeping last-known-good config", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			cancel()
			return nil
		},
	})
}

func warnNotHotReloadable(log *zap.Logger, oldSections, curSections []RestartRequiredSection) {
	current := make(map[string]proto.Message, len(curSections))
	for _, section := range curSections {
		current[section.Name] = section.Message
	}
	for _, old := range oldSections {
		cur, ok := current[old.Name]
		if !ok || !proto.Equal(old.Message, cur) {
			log.Warn("config section changed but requires a restart", zap.String("section", old.Name))
		}
	}
}
