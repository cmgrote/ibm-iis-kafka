/***
 * Copyright 2016 IBM Corp. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

"use strict";

/**
 * Re-usable functions for interacting with IBM InfoSphere Information Server's Kafka event mechanism
 * @module ibm-iis-kafka
 * @license Apache-2.0
 * @requires kafka-node
 * @requires util
 * @requires events
 * @example
 * const iiskafka = require('ibm-iis-kafka');
 * const infosphereEventEmitter = new iiskafka.InfosphereEventEmitter('zookeeper-host:2181', 'asset-object-handler', false);
 * infosphereEventEmitter.on('IGC_DATABASESGROUP_EVENT', function(infosphereEvent, eventCtx, commitCallback) {
 *   console.log("Processing a change to a Database object:");
 *   console.log("  ... type (display name): " + infosphereEvent.ASSET_TYPE);
 *   console.log("  ... unique ID (RID)    : " + infosphereEvent.ASSET_RID);
 *   console.log("  ... parent identity    : " + infosphereEvent.ASSET_CONTEXT);
 *   console.log("  ... asset identity     : " + infosphereEvent.ASSET_NAME);
 *   console.log("  ... action taken       : " + infosphereEvent.ACTION);
 *   console.log("Full event: " + JSON.stringify(infosphereEvent));
 *   commitCallback(eventCtx); // tell Kafka we've successfully consumed this event
 * });
 */

const kafka = require('kafka-node');
const util = require('util');
const EventEmitter = require('events').EventEmitter;

/**
 * Connects to Kafka on the specified system and emits any events raised, based on their eventType
 *
 * @param {string} zookeeperConnection - the hostname of the domain (services) tier of the Information Server environment and port number to connect to Zookeeper service (e.g. hostname:52181)
 * @param {string} handlerId - a unique identity for the handler (allowing multiple handlers to consume the same events)
 * @param {boolean} [bFromBeginning] - if true, process all events from the beginning of tracking in Information Server
 */
function InfosphereEventEmitter(zookeeperConnection, handlerId, bFromBeginning) {

  const self = this;

  if (typeof zookeeperConnection === undefined || zookeeperConnection === "" || zookeeperConnection.indexOf(":") === -1) {
    self.emit('error', "Incomplete connection information -- missing host or port (or both).");
  }

  const client = new kafka.Client(zookeeperConnection, handlerId);
  const consumerOpts = { groupId: handlerId, autoCommit: false, fromOffset: true };
  const payload = { topic: 'InfosphereEvents' };
  const offset = new kafka.Offset(client);

  function commitEvent(kafkaEventCtx, bVerbose) {
    if (bVerbose) {
      console.log(" ... committing offset: " + JSON.stringify(kafkaEventCtx));
    }
    offset.commit(handlerId, [ kafkaEventCtx ], function(err, data) {
      if (typeof err !== 'undefined' && err !== null) {
        self.emit('error', "Unable to commit Kafka event as processed -- " + err + " (" + data + ")");
      }
    });
  }
    
  function emitEvent(message) {
    const infosphereEvent = JSON.parse(message.value);
    const eventCtx = { topic: message.topic, offset: message.offset, partition: message.partition };
    self.emit(infosphereEvent.eventType, infosphereEvent, eventCtx, commitEvent);
  }

  function handleEvents(consumer) {
    consumer.on('message', emitEvent);
    consumer.on('error', function(err) {
      self.emit('error', "Unable to consume messages from Kafka -- " + JSON.stringify(err));
    });
    consumer.on('offsetOutOfRange', function(err) {
      self.emit('error', "Unable to consume messages from Kafka (offsetOutOfRange) -- " + JSON.stringify(err));
    });
    
    process.on('SIGINT', function () {
      consumer.close(true, function () {
        self.emit('end');
        process.exit();
      });
    });
  }

  function _processFromStart() {

    console.log("Consuming all events, from the beginning...");
    // Retrieve the earliest offset for the topic (it will not always be 0)
    let firstEvent = 0;
    offset.fetch([{ topic: 'InfosphereEvents', partition: 0, time: -2, maxNum: 1 }], function (err, data) {
      
      if (err !== null) {
        self.emit('error', "Unable to retrieve an initial offset -- " + err);
      } else {
        firstEvent = data.InfosphereEvents["0"][0];
      }

      payload.offset = firstEvent;
      const consumer = new kafka.Consumer(client, [ payload ], consumerOpts);
      handleEvents(consumer);
  
    });
  }

  if (bFromBeginning) {
    _processFromStart();
  } else {

    // Retrieve the last committed offset for the topic (it will be -1 if the handler is new)
    let lastCommit = -1;
    offset.fetchCommits(handlerId, [{ topic: 'InfosphereEvents', partition: 0 }], function (err, data) {

      if (err !== null) {
        self.emit('error', "Unable to retrieve the last committed offset -- " + err);
      } else {
        lastCommit = data.InfosphereEvents["0"];
      }

      // If we still come back with '-1' as the last committed offset, we need to start at the beginning...
      if (lastCommit === -1) {
        _processFromStart();
      } else {
        console.log("Consuming only events triggered since last consumption (starting at offset: " + lastCommit + ")...");
        payload.offset = lastCommit;
        const consumer = new kafka.Consumer(client, [ payload ], consumerOpts);
        handleEvents(consumer);
      }

    });

  }

}

util.inherits(InfosphereEventEmitter, EventEmitter);

/**
 * This callback is invoked as in order to commit that an event was successfully consumed from Kafka
 * @callback eventCommitCallback
 * @param {Object} eventCtx - the context of the Kafka event that was consumed
 */

if (typeof require === 'function') {
  exports.InfosphereEventEmitter = InfosphereEventEmitter;
}
