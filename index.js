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
 * @file Re-usable functions for interacting with IBM InfoSphere Information Server's Kafka event mechanism
 * @license Apache-2.0
 * @requires kafka-node
 */

/**
 * @module ibm-iis-kafka
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
  const consumerOpts = { groupId: handlerId, autoCommit: false };

  // Retrieve the earliest offset for the topic (it will not always be 0)
  const offset = new kafka.Offset(client);
  let firstEvent = 0;
  offset.fetch([{ topic: 'InfosphereEvents', partition: 0, time: -2, maxNum: 1 }], function (err, data) {
    
    if (err !== null) {
      self.emit('error', "Unable to retrieve an initial offset -- " + err);
    } else {
      firstEvent = data.InfosphereEvents["0"][0];
    }

    if (bFromBeginning) {
      console.log("Consuming all events, from the beginning...");
      consumerOpts.fromOffset = true;
    } else {
      console.log("Consuming only events triggered since last consumption...");
    }

    const consumer = new kafka.Consumer(client, [{ topic: 'InfosphereEvents', offset: firstEvent }], consumerOpts);

    function commitEvent(kafkaEventCtx) {
      console.log(" ... committing offset: " + JSON.stringify(kafkaEventCtx));
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

  });

}

util.inherits(InfosphereEventEmitter, EventEmitter);

/**
 * Connects to Kafka on the specified system and consumes any events raised
 *
 * @param {string} zookeeperConnection - the hostname of the domain (services) tier of the Information Server environment and port number to connect to Zookeeper service (e.g. hostname:52181)
 * @param {EventHandler} handler - the handler that should be used to handle any events consumed
 * @param {boolean} [bFromBeginning] - if true, process all events from the beginning of tracking in Information Server
 */
exports.consumeEvents = function(zookeeperConnection, handler, bFromBeginning) {

  if (typeof zookeeperConnection === undefined || zookeeperConnection === "" || zookeeperConnection.indexOf(":") === -1) {
    throw new Error("Incomplete connection information -- missing host or port (or both).");
  }

  const client = new kafka.Client(zookeeperConnection, handler.id);
  const consumerOpts = { groupId: handler.id, autoCommit: false };

  // Retrieve the earliest offset for the topic (it will not always be 0)
  const offset = new kafka.Offset(client);
  let firstEvent = 0;
  offset.fetch([{ topic: 'InfosphereEvents', partition: 0, time: -2, maxNum: 1 }], function (err, data) {
    
    if (err !== null) {
      throw new Error("Unable to retrieve an initial offset -- " + err);
    } else {
      firstEvent = data.InfosphereEvents["0"][0];
    }

    if (bFromBeginning) {
      console.log("Consuming all events, from the beginning...");
      consumerOpts.fromOffset = true;
    } else {
      console.log("Consuming only events triggered since last consumption...");
    }

    const consumer = new kafka.Consumer(client, [{ topic: 'InfosphereEvents', offset: firstEvent }], consumerOpts);
  
    function processMessage(message) {
      const infosphereEvent = JSON.parse(message.value);
      // Only call out to the handler if the event type is registered as one the handler reacts to
      if (handler.getEventTypes().indexOf(infosphereEvent.eventType) > -1) {
        handler.handleEvent({ topic: message.topic, offset: message.offset, partition: message.partition }, infosphereEvent, checkStatus);
      } else {
        console.log("Ignoring event (" + infosphereEvent.eventType + ") and moving on...");
        commitEvent({ topic: message.topic, offset: message.offset, partition: message.partition });
      }
    }

    function checkStatus(kafkaEventDetails, status) {
      if (typeof status === 'undefined' || status === "") {
        console.error("ERROR: Failed to handle event (" + status + ")");
      } else if (status === "SUCCESS") {
        commitEvent(kafkaEventDetails);
      } else if (status.indexOf("WARN") > -1) {
        console.warn(status);
        commitEvent(kafkaEventDetails);
      }
    }

    function commitEvent(kafkaEventDetails) {
      console.log(" ... committing offset: " + JSON.stringify(kafkaEventDetails));
      offset.commit(handler.id, [ kafkaEventDetails ], function(err, data) {
        if (typeof err !== 'undefined' && err !== null) {
          throw new Error("Unable to commit Kafka event as processed -- " + err + " (" + data + ")");
        }
      });
    }

    consumer.on('message', processMessage);
    consumer.on('error', function(err) {
      throw new Error("Unable to consume messages from Kafka -- " + JSON.stringify(err));
    });
    consumer.on('offsetOutOfRange', function(err) {
      throw new Error("Unable to consume messages from Kafka (offsetOutOfRange) -- " + JSON.stringify(err));
    });
  
    process.on('SIGINT', function () {
      consumer.close(true, function () {
        process.exit();
      });
    });

  });

};

/**
 * @namespace
 */

/**
 * @constructor
 *
 * @param {string} id - a unique identifer for this event handler
 * @param {string[]} eventTypes - an array of the event types this handler wants to monitor
 */
function EventHandler(id, eventTypes) {
  this.id = id;
  this.aEventTypes = eventTypes;
}
EventHandler.prototype = {

  id: null,
  aEventTypes: null,

  /**
   * Retrieve the eventTypes monitored by this EventHandler
   * 
   * @function
   */
  getEventTypes: function() {
    return this.aEventTypes;
  },

  /**
   * Override this function to define how the event should be handled
   * 
   * @param {Object} message - the Kafka message in its entirety
   * @param {Object} infosphereEvent - the Information Server event
   * @function
   */
  handleEvent: function(message, infosphereEvent) { }

};

/**
 * This callback is invoked as the result of consuming an event from Kafka
 * @callback eventCallback
 * @param {Object} kafkaMessage - the full Kafka message consumed
 * @param {Object} infosphereEvent - the InfoSphere Event that was consumed
 */

if (typeof require === 'function') {
  exports.InfosphereEventEmitter = InfosphereEventEmitter;
  exports.EventHandler = EventHandler;
}
