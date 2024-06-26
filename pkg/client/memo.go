package client

import (
	"sync"

	"go.uber.org/zap"
)

// Func is the type of the function to memoize.
type (
	Func          func(string) (interface{}, error)
	EventCallback func(key string, bundlesChannel <-chan interface{}, purgeChannel chan<- string)
)

type command struct {
	op    string
	key   string
	value interface{}
}

type Memo struct {
	eventCallback  EventCallback
	mu             sync.Mutex // guards cache
	channels       map[string]chan interface{}
	logger         *zap.Logger
	purgeChannel   chan string
	operations     chan command
	returnChannels chan chan interface{}
}

func New(
	logger *zap.Logger,
	eventsCallback EventCallback,
) *Memo {
	memo := &Memo{
		eventCallback:  eventsCallback,
		mu:             sync.Mutex{},
		channels:       make(map[string]chan interface{}),
		logger:         logger,
		purgeChannel:   make(chan string),
		operations:     make(chan command),
		returnChannels: make(chan chan interface{}),
	}

	go memo.processCommands()
	go memo.purge()

	return memo
}

func (memo *Memo) Sub(key string) chan interface{} {
	memo.logger.Debug("AAAAA - Memo - Sub - START", zap.String("key", key))
	memo.operations <- command{op: "sub", key: key}
	memo.logger.Debug("AAAAA - Memo - Sub - WAIT for channel", zap.String("key", key))
	resChannel := <-memo.returnChannels
	memo.logger.Debug("AAAAA - Memo - Sub - END", zap.String("key", key))
	return resChannel
}

func (memo *Memo) sub(key string) chan interface{} {
	memo.logger.Debug("AAAAA - Memo - sub - START", zap.String("key", key))
	ch, found := memo.channels[key]
	if !found {
		memo.logger.Debug("AAAAA - Memo - Subscribing to key", zap.String("key", key))

		memo.logger.Debug("AAAAA - Memo - sub - pubsub.sub - before", zap.String("key", key))
		ch := make(chan interface{})
		memo.channels[key] = ch
		memo.logger.Debug("AAAAA - Memo - sub - pubsub.sub - after", zap.String("key", key))
		go memo.eventCallback(key, ch, memo.purgeChannel)
	}
	memo.logger.Debug("AAAAA - Memo - sub - END", zap.String("key", key))
	return ch
}

func (memo *Memo) Pub(key string, value interface{}) {
	memo.logger.Debug("AAAAA - Memo - Pub - START", zap.String("key", key))
	memo.operations <- command{op: "pub", key: key, value: value}
	memo.logger.Debug("AAAAA - Memo - Pub - END", zap.String("key", key))
}

func (memo *Memo) pub(key string, value interface{}) {
	memo.logger.Debug("AAAAA - Memo - pub - START", zap.String("key", key))
	ch, found := memo.channels[key]
	if found {
		ch <- value
	} else {
		memo.logger.Warn("AAAAA - Memo - pub - does not exist", zap.String("key", key))
	}
	memo.logger.Debug("AAAAA - Memo - pub - END", zap.String("key", key))
}

func (memo *Memo) unsub(key string) {
	memo.logger.Debug("AAAAA - Memo - unsub - START", zap.String("key", key))
	ch, found := memo.channels[key]
	if found {
		memo.logger.Debug("AAAAA - Memo - Unsubscribing to key", zap.String("key", key))
		delete(memo.channels, key)
		memo.logger.Debug("AAAAA - Memo - close - before", zap.String("key", key))
		close(ch)
		memo.logger.Debug("AAAAA - Memo - close - after", zap.String("key", key))
	} else {
		memo.logger.Warn("AAAAA - Memo - Unsubscribing to key - already unsubscribed", zap.String("key", key))
	}
	memo.logger.Debug("AAAAA - Memo - unsub - END", zap.String("key", key))
}

func (memo *Memo) processCommands() {
	memo.logger.Debug("AAAAA - Memo - processCommands - START")
	for {
		memo.logger.Debug("AAAAA - Memo - processCommands - WAITING")
		cmd := <-memo.operations
		memo.logger.Debug("AAAAA - Memo - processCommands - START", zap.String("cmd", cmd.op), zap.String("key", cmd.key))
		switch cmd.op {
		case "sub":
			ch := memo.sub(cmd.key)
			memo.logger.Debug("AAAAA - Memo - processCommands - sub - before return channel", zap.String("cmd", cmd.op), zap.String("key", cmd.key))
			memo.returnChannels <- ch
			memo.logger.Debug("AAAAA - Memo - processCommands - sub - after return channel", zap.String("cmd", cmd.op), zap.String("key", cmd.key))
		case "unsub":
			memo.unsub(cmd.key)
		case "pub":
			memo.pub(cmd.key, cmd.value)
		}
		memo.logger.Debug("AAAAA - Memo - processCommands - END", zap.String("cmd", cmd.op), zap.String("key", cmd.key))
	}
}

func (memo *Memo) purge() {
	for {
		memo.logger.Debug("AAAAA - Memo - purge - WAITING")
		key, ok := <-memo.purgeChannel
		if !ok {
			memo.logger.Info("Purge channel closed")
			return
		}
		memo.logger.Debug("AAAAA - Memo - purge - START", zap.String("key", key))
		memo.logger.Info("Purging key", zap.String("key", key))
		memo.operations <- command{op: "unsub", key: key}
		memo.logger.Debug("AAAAA - Memo - purge - END", zap.String("key", key))
	}
}

//!-
