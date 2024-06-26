package session_manager

import (
	"sync"

	"go.uber.org/zap"
)

type (
	Func          func(string) (interface{}, error)
	EventCallback func(key string, bundlesChannel <-chan interface{}, purgeChannel chan<- string)
)

type command struct {
	op    string
	key   string
	value interface{}
}

type SessionManager struct {
	eventCallback EventCallback
	mu            sync.Mutex // guards cache
	channels      map[string]chan interface{}
	logger        *zap.Logger
	purgeChannel  chan string
	operations    chan command
}

func New(
	logger *zap.Logger,
	eventsCallback EventCallback,
) *SessionManager {
	manager := &SessionManager{
		eventCallback: eventsCallback,
		mu:            sync.Mutex{},
		channels:      make(map[string]chan interface{}),
		logger:        logger,
		purgeChannel:  make(chan string),
		operations:    make(chan command),
	}

	go manager.processCommands()
	go manager.purge()

	return manager
}

func (manager *SessionManager) Sub(key string) {
	manager.operations <- command{op: "sub", key: key}
}

func (manager *SessionManager) sub(key string) {
	_, found := manager.channels[key]
	if !found {
		ch := make(chan interface{})
		manager.channels[key] = ch
		go manager.eventCallback(key, ch, manager.purgeChannel)
	}
}

func (manager *SessionManager) Pub(key string, value interface{}) {
	manager.operations <- command{op: "pub", key: key, value: value}
}

func (manager *SessionManager) pub(key string, value interface{}) {
	ch, found := manager.channels[key]
	if found {
		ch <- value
	} else {
		manager.logger.Warn("Channel for publishing does not exist", zap.String("key", key))
	}
}

func (manager *SessionManager) unsub(key string) {
	ch, found := manager.channels[key]
	if found {
		manager.logger.Debug("Unsubscribing to key", zap.String("key", key))
		delete(manager.channels, key)
		close(ch)
	} else {
		manager.logger.Warn("Unsubscribing to key - already unsubscribed", zap.String("key", key))
	}
}

func (manager *SessionManager) processCommands() {
	for {
		cmd := <-manager.operations
		manager.logger.Debug("SessionManager - processCommands - START", zap.String("cmd", cmd.op), zap.String("key", cmd.key))
		switch cmd.op {
		case "sub":
			manager.sub(cmd.key)
		case "unsub":
			manager.unsub(cmd.key)
		case "pub":
			manager.pub(cmd.key, cmd.value)
		}
		manager.logger.Debug("SessionManager - processCommands - END", zap.String("cmd", cmd.op), zap.String("key", cmd.key))
	}
}

func (manager *SessionManager) purge() {
	for {
		key, ok := <-manager.purgeChannel
		if !ok {
			manager.logger.Info("Purge channel closed")
			return
		}
		manager.logger.Info("Purging key", zap.String("key", key))
		manager.operations <- command{op: "unsub", key: key}
	}
}
