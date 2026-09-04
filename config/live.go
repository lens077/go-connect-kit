package config

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
)

type liveValue[T proto.Message] struct {
	value T
}

// Live holds the current configuration and atomically replaces it as a whole.
type Live[T proto.Message] struct {
	ptr atomic.Pointer[liveValue[T]]

	applyMu sync.Mutex
	mu      sync.Mutex
	next    int
	subs    map[int]func(old, cur T)

	source   Source
	decode   func(map[string]any, T) error
	validate func(T) error
}

// NewLive creates a standalone Live value. A typed nil is normalized to a new
// empty message so chained generated getters remain safe.
func NewLive[T proto.Message](initial T) *Live[T] {
	if isNil(initial) {
		initial = newMessage[T]()
	}
	live := &Live[T]{
		subs: make(map[int]func(old, cur T)),
	}
	live.decode = func(data map[string]any, target T) error {
		return decodeConfig(data, target, LoadOptions{})
	}
	live.validate = validateMessage[T]
	live.ptr.Store(&liveValue[T]{value: initial})
	return live
}

// Get returns the current read-only configuration pointer.
func (l *Live[T]) Get() T {
	return l.ptr.Load().value
}

// Set replaces the current configuration and synchronously notifies subscribers.
func (l *Live[T]) Set(cur T) {
	if isNil(cur) {
		return
	}

	l.applyMu.Lock()
	defer l.applyMu.Unlock()

	old := l.ptr.Swap(&liveValue[T]{value: cur}).value

	l.mu.Lock()
	callbacks := make([]func(old, cur T), 0, len(l.subs))
	for _, callback := range l.subs {
		callbacks = append(callbacks, callback)
	}
	l.mu.Unlock()

	for _, callback := range callbacks {
		callback(old, cur)
	}
}

// Subscribe registers a synchronous update callback and returns an idempotent cancel function.
func (l *Live[T]) Subscribe(callback func(old, cur T)) func() {
	l.mu.Lock()
	id := l.next
	l.next++
	l.subs[id] = callback
	l.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			delete(l.subs, id)
			l.mu.Unlock()
		})
	}
}

// SourceName returns the source selected for this Live value.
func (l *Live[T]) SourceName() string {
	if l.source == nil {
		return ""
	}
	return l.source.Name()
}

func newMessage[T proto.Message]() T {
	var zero T
	typ := reflect.TypeOf(zero)
	if typ == nil || typ.Kind() != reflect.Pointer {
		panic(fmt.Sprintf("config message type %T must be a concrete pointer", zero))
	}
	message, ok := reflect.New(typ.Elem()).Interface().(T)
	if !ok {
		panic(fmt.Sprintf("cannot construct config message type %v", typ))
	}
	return message
}

func isNil[T any](value T) bool {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return true
	}
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
