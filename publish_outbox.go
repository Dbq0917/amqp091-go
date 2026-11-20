package amqp091

import "sync"

type pendingPublish struct {
	exchange  string
	key       string
	mandatory bool
	immediate bool
	msg       Publishing
	tag       uint64
	dc        *DeferredConfirmation
}

func (p *pendingPublish) message() *basicPublish {
	return &basicPublish{
		Exchange:   p.exchange,
		RoutingKey: p.key,
		Mandatory:  p.mandatory,
		Immediate:  p.immediate,
		Body:       p.msg.Body,
		Properties: propertiesFromPublishing(p.msg),
	}
}

type publishOutbox struct {
	mu      sync.Mutex
	pending []*pendingPublish
	byTag   map[uint64]*pendingPublish
}

func (o *publishOutbox) add(p *pendingPublish) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.pending = append(o.pending, p)
}

func (o *publishOutbox) bind(p *pendingPublish, tag uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	p.tag = tag
	if o.byTag == nil {
		o.byTag = make(map[uint64]*pendingPublish)
	}
	o.byTag[tag] = p
}

func (o *publishOutbox) discard(p *pendingPublish) {
	o.mu.Lock()
	defer o.mu.Unlock()

	delete(o.byTag, p.tag)
	for i, candidate := range o.pending {
		if candidate == p {
			copy(o.pending[i:], o.pending[i+1:])
			o.pending[len(o.pending)-1] = nil
			o.pending = o.pending[:len(o.pending)-1]
			return
		}
	}
}

func (o *publishOutbox) ack(tag uint64, multiple bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if multiple {
		kept := o.pending[:0]
		for _, p := range o.pending {
			if p.tag != 0 && p.tag <= tag {
				delete(o.byTag, p.tag)
				continue
			}
			kept = append(kept, p)
		}
		o.pending = kept
		return
	}

	p, ok := o.byTag[tag]
	if !ok {
		return
	}
	delete(o.byTag, tag)
	for i, candidate := range o.pending {
		if candidate == p {
			copy(o.pending[i:], o.pending[i+1:])
			o.pending[len(o.pending)-1] = nil
			o.pending = o.pending[:len(o.pending)-1]
			return
		}
	}
}

func (o *publishOutbox) snapshot() []*pendingPublish {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]*pendingPublish(nil), o.pending...)
}

func (o *publishOutbox) reset() {
	o.mu.Lock()
	defer o.mu.Unlock()

	for _, p := range o.pending {
		p.tag = 0
		p.dc = nil
	}
	o.byTag = nil
}

func (o *publishOutbox) close() {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.pending = nil
	o.byTag = nil
}

func clonePublishingForRetry(msg Publishing) Publishing {
	msg.Body = append([]byte(nil), msg.Body...)
	msg.Headers = cloneTableForRetry(msg.Headers)
	return msg
}

func cloneTableForRetry(table Table) Table {
	if table == nil {
		return nil
	}
	clone := make(Table, len(table))
	for key, value := range table {
		clone[key] = cloneTableValueForRetry(value)
	}
	return clone
}

func cloneTableValueForRetry(value any) any {
	switch value := value.(type) {
	case Table:
		return cloneTableForRetry(value)
	case []any:
		clone := make([]any, len(value))
		for i, item := range value {
			clone[i] = cloneTableValueForRetry(item)
		}
		return clone
	case []byte:
		return append([]byte(nil), value...)
	default:
		return value
	}
}
