package client

import (
	"sync"

	"github.com/cskr/pubsub"
	"go.uber.org/zap"
)

// Func is the type of the function to memoize.
type (
	Func          func(string) (interface{}, error)
	EventCallback func(key string, bundlesChannel <-chan interface{}, purgeChannel chan<- string)
)

// !+
type entry struct {
	channel  chan interface{}
	subReady chan struct{} // closed when res is ready
}

type Memo struct {
	ps            *pubsub.PubSub
	eventCallback EventCallback
	mu            sync.Mutex // guards cache
	channels      map[string]*entry
	logger        *zap.Logger
	purgeChannel  chan string
}

func New(
	logger *zap.Logger,
	eventsCallback EventCallback,
) *Memo {
	memo := &Memo{
		ps:            pubsub.New(0),
		eventCallback: eventsCallback,
		mu:            sync.Mutex{},
		channels:      make(map[string]*entry),
		logger:        logger,
		purgeChannel:  make(chan string),
	}

	go memo.purge()

	return memo
}

func (memo *Memo) Sub(key string) chan interface{} {
	memo.logger.Debug("AAAAA - Memo - Sub - before lock", zap.String("key", key))
	memo.mu.Lock()
	memo.logger.Debug("AAAAA - Memo - Sub - after lock", zap.String("key", key))
	e := memo.channels[key]
	if e == nil {
		memo.logger.Debug("Subscribing to key", zap.String("key", key))
		// This is the first request for this key.
		// This goroutine becomes responsible for computing
		// the value and broadcasting the ready condition.
		e = &entry{subReady: make(chan struct{})}
		memo.channels[key] = e

		memo.logger.Debug("AAAAA - Memo - Sub - pubsub.sub - before", zap.String("key", key))
		// here is the pub sub adding
		e.channel = memo.ps.Sub(key)
		memo.logger.Debug("AAAAA - Memo - Sub - pubsub.sub - after", zap.String("key", key))

		memo.logger.Debug("AAAAA - Memo - Sub - before unlock", zap.String("key", key))
		memo.mu.Unlock()
		memo.logger.Debug("AAAAA - Memo - Sub - after unlock", zap.String("key", key))

		go memo.eventCallback(key, e.channel, memo.purgeChannel)

		close(e.subReady) // broadcast ready condition
	} else {
		// This is a repeat request for this key.
		memo.logger.Debug("AAAAA - Memo - Sub - before unlock", zap.String("key", key))
		memo.mu.Unlock()
		memo.logger.Debug("AAAAA - Memo - Sub - after unlock", zap.String("key", key))

		<-e.subReady // wait for ready condition
	}
	memo.logger.Debug("AAAAA - Memo - Sub - END", zap.String("key", key))
	return e.channel
}

func (memo *Memo) Pub(key string, value interface{}) {
	memo.ps.Pub(value, key)
}

func (memo *Memo) unsub(key string) {
	memo.logger.Debug("AAAAA - Memo - unsub - before lock", zap.String("key", key))
	memo.mu.Lock()
	memo.logger.Debug("AAAAA - Memo - unsub - after lock", zap.String("key", key))
	e := memo.channels[key]
	if e == nil {
		memo.mu.Unlock()
		return
	} else {
		memo.logger.Debug("Unsubscribing to key", zap.String("key", key))
		delete(memo.channels, key)
		memo.logger.Debug("AAAAA - Memo - pubsub.unsub - before", zap.String("key", key))
		// This is not necessary, and may cause problems
		go memo.ps.Unsub(e.channel)
		memo.logger.Debug("AAAAA - Memo - pubsub.unsub - after", zap.String("key", key))
		memo.mu.Unlock()
	}
}

func (memo *Memo) purge() {
	for {
		key, ok := <-memo.purgeChannel
		if !ok {
			memo.logger.Info("Purge channel closed")
			return
		}
		memo.logger.Info("Purging key", zap.String("key", key))
		memo.unsub(key)
	}
}

//!-
