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

/**
 * @file Re-usable functions for interacting with IBM InfoSphere Information Server's Kafka event mechanism
 * @license Apache-2.0
 * @requires kafka-node
 */

/**
 * @module ibm-iis-kafka
 */

const https = require('https');
const kafka = require('kafka-node');

/*
var host = "";
var port = "";

/**
 * Set access details for the Kafka connection
 *
 * @param {string} host - the hostname of the domain (services) tier of the Information Server environment
 * @param {string} port - the port number of the Zookeeper service (e.g. 52181)
exports.setServer = function(host, port) {
  if (host === undefined || host === "" || port === undefined || port === "") {
    throw new Error("Incomplete connection information -- missing host or port (or both).");
  }
  this.host = host;
  this.port = port;
}
 */

/**
 * Connects to Kafka on the specified system and consumes any events raised
 *
 * @param {string} zookeeperConnection - the hostname of the domain (services) tier of the Information Server environment and port number to connect to Zookeeper service (e.g. hostname:52181)
 * @param {EventHandler} handler - the handler that should be used to handle any events consumed
 * @param {boolean} [bFromBeginning] - if true, process all events from the beginning of tracking in Information Server
 */
exports.consumeEvents = function(zookeeperConnection, handler, bFromBeginning) {

  if (zookeeperConnection === undefined || zookeeperConnection === "" || zookeeperConnection.indexOf(":") == -1) {
    throw new Error("Incomplete connection information -- missing host or port (or both).");
  }
  var Consumer = kafka.Consumer;
  var client = new kafka.Client(zookeeperConnection, handler.id);
  var consumer = null;
  if (bFromBeginning) {
    console.log("Consuming all events, from the beginning...");
    consumer = new Consumer(client, [{ topic: 'InfosphereEvents', offset: 0 }], {groupId: handler.id, autoCommit: false, fromOffset: true});
  } else {
    consumer = new Consumer(client, [{ topic: 'InfosphereEvents', offset: 0 }], {groupId: handler.id, autoCommit: false, fromOffset: false});
  }
  consumer.on('message', function(message) {
    var infosphereEvent = JSON.parse(message.value);
    // Only call out to the handler if the event type is registered as one the handler reacts to
    if (handler.getEventTypes().indexOf(infosphereEvent.eventType) > -1) {
      handler.handleEvent(message, infosphereEvent, function(status) {
        if (status === undefined || status === "") {
          console.error("ERROR: Failed to handle event (" + status + ")");
        } else if (status === "SUCCESS") {
          _commitEvent(consumer);
        } else if (status.indexOf("WARN") > -1) {
          console.warn(status);
          _commitEvent(consumer);
        }
      });
    } else {
      console.log("Ignoring event (" + infosphereEvent.eventType + ") and moving on...");
      _commitEvent(consumer);
    }
  });

  process.on('SIGINT', function () {
    consumer.close(true, function () {
        process.exit();
    });
  });

}

/**
 * @private
 */
function _commitEvent(consumer) {
  consumer.commit(function(err, data) {
    if (err !== null) {
      console.error("ERROR: Unable to commit Kafka event processing -- " + err);
      console.error(data);
    }
  });
}

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

if (typeof require == 'function') {
  exports.EventHandler = EventHandler;
}
