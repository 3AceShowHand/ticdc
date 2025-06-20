// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package eventcollector

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// dispatcherStat is a helper struct to manage the state of a dispatcher.
type dispatcherStat struct {
	dispatcherID common.DispatcherID
	target       dispatcher.EventDispatcher

	readyCallback func()

	eventServiceInfo struct {
		sync.RWMutex
		// the server this dispatcher currently connects to(except local event service)
		// if it is set to local event service id, ignore all messages from other event service
		serverID node.ID
		// whether has received ready signal from `serverID`
		readyEventReceived bool
		// the remote event services which may contain data this dispatcher needed
		remoteCandidates []node.ID
	}

	// lastEventSeq is the sequence number of the last received DML/DDL/Handshake event.
	// It is used to ensure the order of events.
	lastEventSeq atomic.Uint64

	// waitHandshake is used to indicate whether the dispatcher is waiting for a handshake event.
	// Dispatcher will drop all data events before receiving a handshake event.
	waitHandshake atomic.Bool

	// The largest commit ts that has been sent to the dispatcher.
	sentCommitTs atomic.Uint64

	// tableInfo is the latest table info of the dispatcher's corresponding table.
	tableInfo atomic.Value
}

func (d *dispatcherStat) reset() {
	d.lastEventSeq.Store(0)
	d.waitHandshake.Store(true)
}

// TODO: add epoch to event and use it to filter irrelevant events
func (d *dispatcherStat) isEventFromCurrentEventService(event dispatcher.DispatcherEvent) bool {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()
	if *event.From != d.eventServiceInfo.serverID {
		log.Warn("Receive event from other event service, ignore it",
			zap.Stringer("dispatcher", d.target.GetId()))
		return false
	}
	// TODO: maybe we can remove this after add epoch?
	if d.waitHandshake.Load() {
		log.Warn("Receive event before handshake event, ignore it",
			zap.Stringer("dispatcher", d.target.GetId()))
		return false
	}
	return true
}

// isEventSeqValid check whether there are any events being dropped
func (d *dispatcherStat) isEventSeqValid(event dispatcher.DispatcherEvent) bool {
	switch event.GetType() {
	case commonEvent.TypeDMLEvent,
		commonEvent.TypeDDLEvent,
		commonEvent.TypeHandshakeEvent:
		log.Debug("check event sequence",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Int("eventType", event.GetType()),
			zap.Uint64("receivedSeq", event.GetSeq()),
			zap.Uint64("lastEventSeq", d.lastEventSeq.Load()),
			zap.Uint64("commitTs", event.GetCommitTs()))

		expectedSeq := d.lastEventSeq.Add(1)
		if event.GetSeq() != expectedSeq {
			log.Warn("Received an out-of-order event, reset the dispatcher",
				zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Int("eventType", event.GetType()),
				zap.Uint64("receivedSeq", event.GetSeq()),
				zap.Uint64("expectedSeq", expectedSeq),
				zap.Uint64("commitTs", event.GetCommitTs()))
			return false
		}
	}
	return true
}

func (d *dispatcherStat) isEventCommitTsValid(event dispatcher.DispatcherEvent) bool {
	// Note: a commit ts may have multiple transactions.
	// it is ok to send the same txn multiple times?
	// (we just want to avoid send old dml after new ddl)
	if event.GetCommitTs() < d.sentCommitTs.Load() {
		log.Warn("Receive a event older than sendCommitTs, ignore it",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Int64("tableID", d.target.GetTableSpan().TableID),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Any("event", event.Event),
			zap.Uint64("eventCommitTs", event.GetCommitTs()),
			zap.Uint64("sentCommitTs", d.sentCommitTs.Load()))
		return false
	}
	d.sentCommitTs.Store(event.GetCommitTs())
	return true
}

func (d *dispatcherStat) handleHandshakeEvent(event dispatcher.DispatcherEvent, eventCollector *EventCollector) {
	d.eventServiceInfo.Lock()
	defer d.eventServiceInfo.Unlock()
	if event.GetType() != commonEvent.TypeHandshakeEvent {
		log.Panic("should not happen")
	}
	if d.eventServiceInfo.serverID == "" {
		log.Panic("should not happen: server ID is not set")
	}
	if d.eventServiceInfo.serverID != *event.From {
		// check invariant: if the handshake event is not from the current event service, we must be reading from local event service.
		if d.eventServiceInfo.serverID != eventCollector.serverId {
			log.Panic("receive handshake event from remote event service, but current event service is not local event service",
				zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Stringer("from", event.From))
		}
		log.Info("receive handshake event from remote event service, but current event service is local event service, ignore it",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Stringer("from", event.From))
		return
	}
	if !d.isEventSeqValid(event) {
		eventCollector.resetDispatcher(d)
		return
	}
	d.waitHandshake.Store(false)
}

func (d *dispatcherStat) setTableInfo(tableInfo *common.TableInfo) {
	if tableInfo == nil {
		return
	}
	d.tableInfo.Store(tableInfo)
}

func (d *dispatcherStat) handleReadyEvent(event dispatcher.DispatcherEvent, eventCollector *EventCollector) {
	d.eventServiceInfo.Lock()
	defer d.eventServiceInfo.Unlock()

	if event.GetType() != commonEvent.TypeReadyEvent {
		log.Panic("should not happen")
	}
	if d.eventServiceInfo.serverID == eventCollector.serverId {
		// already received ready signal from local event service
		return
	}
	// if a dispatcher's readyCallback is set, it will just register to local event service.
	if d.readyCallback != nil {
		d.eventServiceInfo.serverID = eventCollector.serverId
		d.eventServiceInfo.readyEventReceived = true
		d.readyCallback()
		return
	}
	eventServiceID := *event.From
	if d.eventServiceInfo.serverID == eventServiceID {
		// case 1: already received ready signal from the same server
		if d.eventServiceInfo.readyEventReceived {
			log.Info("received ready signal from the same server again, ignore it",
				zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Stringer("eventServiceID", eventServiceID))
			return
		}
		// case 2: first ready signal from the server
		// (must be a remote candidate, because we won't set d.eventServiceInfo.serverID to local event service until we receive ready signal)
		log.Info("received ready signal from remote event service, prepare to reset the dispatcher",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Stringer("eventServiceID", eventServiceID))

		d.eventServiceInfo.serverID = eventServiceID
		d.eventServiceInfo.readyEventReceived = true
		eventCollector.addDispatcherRequestToSendingQueue(
			eventServiceID,
			eventServiceTopic,
			DispatcherRequest{
				Dispatcher: d.target,
				StartTs:    d.sentCommitTs.Load(),
				ActionType: eventpb.ActionType_ACTION_TYPE_RESET,
			},
		)
	} else if eventServiceID == eventCollector.serverId {
		// case 3: received first ready signal from local event service
		if d.eventServiceInfo.serverID != "" {
			eventCollector.addDispatcherRequestToSendingQueue(
				d.eventServiceInfo.serverID,
				eventServiceTopic,
				DispatcherRequest{
					Dispatcher: d.target,
					ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
				},
			)
		}
		log.Info("received ready signal from local event service, prepare to reset the dispatcher",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Stringer("eventServiceID", eventServiceID))

		d.eventServiceInfo.serverID = eventServiceID
		d.eventServiceInfo.readyEventReceived = true
		d.eventServiceInfo.remoteCandidates = nil
		eventCollector.addDispatcherRequestToSendingQueue(
			eventServiceID,
			eventServiceTopic,
			DispatcherRequest{
				Dispatcher: d.target,
				StartTs:    d.sentCommitTs.Load(),
				ActionType: eventpb.ActionType_ACTION_TYPE_RESET,
			},
		)
	} else {
		log.Panic("should not happen: we have received ready signal from other remote server",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Stringer("newRemoteEventService", eventServiceID),
			zap.Stringer("oldRemoteEventService", d.eventServiceInfo.serverID))
	}
}

func (d *dispatcherStat) handleNotReusableEvent(event dispatcher.DispatcherEvent, eventCollector *EventCollector) {
	d.eventServiceInfo.Lock()
	defer d.eventServiceInfo.Unlock()
	if event.GetType() != commonEvent.TypeNotReusableEvent {
		log.Panic("should not happen")
	}
	if *event.From == d.eventServiceInfo.serverID {
		if len(d.eventServiceInfo.remoteCandidates) > 0 {
			eventCollector.addDispatcherRequestToSendingQueue(
				d.eventServiceInfo.remoteCandidates[0],
				eventServiceTopic,
				DispatcherRequest{
					Dispatcher: d.target,
					StartTs:    d.target.GetStartTs(),
					ActionType: eventpb.ActionType_ACTION_TYPE_REGISTER,
					OnlyUse:    true,
				},
			)
			d.eventServiceInfo.serverID = d.eventServiceInfo.remoteCandidates[0]
			d.eventServiceInfo.remoteCandidates = d.eventServiceInfo.remoteCandidates[1:]
		}
	}
}

func (d *dispatcherStat) unregisterDispatcher(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()
	// must unregister from local event service
	eventCollector.mustSendDispatcherRequest(eventCollector.serverId, eventServiceTopic, DispatcherRequest{
		Dispatcher: d.target,
		ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
	})
	// unregister from remote event service if have
	if d.eventServiceInfo.serverID != "" && d.eventServiceInfo.serverID != eventCollector.serverId {
		eventCollector.mustSendDispatcherRequest(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
			Dispatcher: d.target,
			ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
		})
	}
}

func (d *dispatcherStat) pauseDispatcher(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()

	if d.eventServiceInfo.serverID == "" || !d.eventServiceInfo.readyEventReceived {
		log.Info("ignore pause dispatcher request because the eventService is not ready",
			zap.Stringer("dispatcherID", d.dispatcherID),
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Any("eventServiceID", d.eventServiceInfo.serverID),
			zap.Bool("readyEventReceived", d.eventServiceInfo.readyEventReceived),
		)
		// Just ignore the request if the dispatcher is not ready.
		return
	}

	eventCollector.addDispatcherRequestToSendingQueue(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
		Dispatcher: d.target,
		ActionType: eventpb.ActionType_ACTION_TYPE_PAUSE,
	})
}

func (d *dispatcherStat) resumeDispatcher(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()

	if d.eventServiceInfo.serverID == "" || !d.eventServiceInfo.readyEventReceived {
		log.Info("ignore resume dispatcher request because the eventService is not ready",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Any("eventServiceID", d.eventServiceInfo.serverID))
		// Just ignore the request if the dispatcher is not ready.
		return
	}

	eventCollector.addDispatcherRequestToSendingQueue(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
		Dispatcher: d.target,
		ActionType: eventpb.ActionType_ACTION_TYPE_RESUME,
	})
}

// TODO: better name
func (d *dispatcherStat) setRemoteCandidates(nodes []string, eventCollector *EventCollector) {
	log.Info("set remote candidates",
		zap.Strings("nodes", nodes),
		zap.Stringer("dispatcherID", d.target.GetId()))
	if len(nodes) == 0 {
		return
	}
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()
	// reading from a event service or checking remotes already, ignore
	if d.eventServiceInfo.serverID != "" {
		return
	}
	d.eventServiceInfo.serverID = node.ID(nodes[0])
	for i := 1; i < len(nodes); i++ {
		d.eventServiceInfo.remoteCandidates = append(d.eventServiceInfo.remoteCandidates, node.ID(nodes[i]))
	}
	eventCollector.addDispatcherRequestToSendingQueue(
		d.eventServiceInfo.serverID,
		eventServiceTopic,
		DispatcherRequest{
			Dispatcher: d.target,
			StartTs:    d.target.GetStartTs(),
			ActionType: eventpb.ActionType_ACTION_TYPE_REGISTER,
			OnlyUse:    true,
		},
	)
}

func (d *dispatcherStat) getServerID() node.ID {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()
	return d.eventServiceInfo.serverID
}
