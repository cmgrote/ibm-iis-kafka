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

var https = require('https');
var kafka = require('kafka-node');

/**
 * Connects to Kafka on the specified system and consumes any events raised
 *
 * @param {string} zookeeperConnection - the hostname of the domain (services) tier of the Information Server environment and port number to connect to Zookeeper service (e.g. hostname:52181)
 * @param {EventHandler} handler - the handler that should be used to handle any events consumed
 * @param {boolean} [bFromBeginning] - if true, process all events from the beginning of tracking in Information Server
 */
exports.consumeEvents = function(zookeeperConnection, handler, bFromBeginning) {

  var Consumer;
  var client;
  var consumerOpts;
  var consumer;
  var offset;
  var firstEvent = 0;

  if (typeof zookeeperConnection === undefined || zookeeperConnection === "" || zookeeperConnection.indexOf(":") == -1) {
    throw new Error("Incomplete connection information -- missing host or port (or both).");
  }

  client = new kafka.Client(zookeeperConnection, handler.id);
  consumerOpts = { groupId: handler.id, autoCommit: false };

  // Retrieve the earliest offset for the topic (it will not always be 0)
  offset = new kafka.Offset(client);
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

    consumer = new kafka.Consumer(client, [{ topic: 'InfosphereEvents', offset: firstEvent }], consumerOpts);

    function commitEvent() {
      consumer.commit(function(err, data) {
        if (err !== null) {
          console.error("ERROR: Unable to commit Kafka event processing -- " + err);
          console.error(data);
        }
      });
    }
  
    function checkStatus(status) {
      if (typeof status === undefined || status === "") {
        console.error("ERROR: Failed to handle event (" + status + ")");
      } else if (status === "SUCCESS") {
        commitEvent();
      } else if (status.indexOf("WARN") > -1) {
        console.warn(status);
        commitEvent();
      }
    }
  
    function processMessage(message) {
      var infosphereEvent = JSON.parse(message.value);
      // Only call out to the handler if the event type is registered as one the handler reacts to
      if (handler.getEventTypes().indexOf(infosphereEvent.eventType) > -1) {
        handler.handleEvent(message, infosphereEvent, checkStatus);
      } else {
        console.log("Ignoring event (" + infosphereEvent.eventType + ") and moving on...");
        commitEvent();
      }
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
  exports.EventHandler = EventHandler;
}
