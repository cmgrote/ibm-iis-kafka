#!/usr/bin/env node

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
 * @file Builds a fully-detailed object (including all related information) based on any changes to DQ-related objects
 * @license Apache-2.0
 * @requires ibm-igc-rest
 * @requires ibm-iis-kafka
 * @requires yargs
 * @example
 * // monitors any DQ-related object changes on hostname, and constructs fully-detailed objects whenever a DQ-related object changes
 * ./consumeDQObjectChanges.js -d hostname:9445 -z hostname:52181
 */

var igcrest = require('ibm-igc-rest');
var iiskafka = require('../');

// Command-line setup
var yargs = require('yargs');
var argv = yargs
    .usage('Usage: $0 -d <host>:<port> -z <host>:<port> -u <username> -p <password>')
    .env('DS')
    .option('d', {
      alias: 'domain',
      describe: 'Host and port for invoking IGC REST',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('z', {
      alias: 'zookeeper',
      describe: 'Host and port for Zookeeper connection to consume from Kafka',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('u', {
      alias: 'deployment-user',
      describe: 'User for invoking IGC REST',
      demand: true, requiresArg: true, type: 'string',
      default: "isadmin"
    })
    .option('p', {
      alias: 'deployment-user-password',
      describe: 'Password for invoking IGC REST',
      demand: true, requiresArg: true, type: 'string',
      default: "isadmin"
    })
    .help('h')
    .alias('h', 'help')
    .wrap(yargs.terminalWidth())
    .argv;

// Base settings
var host_port = argv.domain.split(":");
var dqHandler = new iiskafka.EventHandler('dq-handler', ['NEW_EXCEPTIONS_EVENT']);

igcrest.setAuth(argv.deploymentUser, argv.deploymentUserPassword);
igcrest.setServer(host_port[0], host_port[1]);

dqHandler.handleEvent = function(message, infosphereEvent) {

  var tableName;
  var ruleName;

  if (infosphereEvent.eventType === "NEW_EXCEPTIONS_EVENT") {

    if (infosphereEvent.applicationType === "Exception Stage") {

      tableName = infosphereEvent.exceptionSummaryUID;
      console.log("Table: " + tableName);

    } else if (infosphereEvent.applicationType === "Information Analyzer") {

      ruleName = infosphereEvent.exceptionSummaryName;
      console.log("Rule : " + ruleName);

    }

  }

};

iiskafka.consumeEvents(argv.zookeeper, dqHandler, false);
