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

"use strict";

/**
 * @file Retrieves both summary statistics and detailed record failures based on any Data Quality exception events
 * @license Apache-2.0
 * @requires ibm-ia-rest
 * @requires ibm-iis-kafka
 * @requires yargs
 * @example
 * // monitors all exception events on hostname, and outputs summary statistics and detailed records
 * ./consumeDQExceptions.js -d hostname:9445 -z hostname:52181
 */

const iarest = require('ibm-ia-rest');
const iiskafka = require('../');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
    .usage('Usage: $0 -d <host>:<port> -z <host>:<port> -u <username> -p <password>')
    .env('DS')
    .option('d', {
      alias: 'domain',
      describe: 'Host and port for invoking IA REST',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('z', {
      alias: 'zookeeper',
      describe: 'Host and port for Zookeeper connection to consume from Kafka',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('u', {
      alias: 'deployment-user',
      describe: 'User for invoking IA REST',
      demand: true, requiresArg: true, type: 'string',
      default: "isadmin"
    })
    .option('p', {
      alias: 'deployment-user-password',
      describe: 'Password for invoking IA REST',
      demand: true, requiresArg: true, type: 'string',
      default: "isadmin"
    })
    .help('h')
    .alias('h', 'help')
    .wrap(yargs.terminalWidth())
    .argv;

// Base settings
const host_port = argv.domain.split(":");

iarest.setAuth(argv.deploymentUser, argv.deploymentUserPassword);
iarest.setServer(host_port[0], host_port[1]);

const infosphereEventEmitter = new iiskafka.InfosphereEventEmitter(argv.zookeeper, 'dq-handler', true);

infosphereEventEmitter.on('NEW_EXCEPTIONS_EVENT', processFailedRecords);
infosphereEventEmitter.on('error', function(errMsg) {
  console.error("Received 'error' -- aborting process: " + errMsg);
  process.exit(1);
});
infosphereEventEmitter.on('end', function() {
  console.log("Event emitter stopped -- ending process.");
  process.exit();
});

function handleError(ctxMsg, errMsg) {
  if (typeof errMsg !== 'undefined' && errMsg !== null) {
    console.error("Failed " + ctxMsg + " -- " + errMsg);
    process.exit(1);
  }
}

function processFailedRecords(infosphereEvent, eventCtx, commitCallback) {

  // DataStage-generated failed records...
  if (infosphereEvent.applicationType === "Exception Stage") {

    let tableName = infosphereEvent.exceptionSummaryUID;
    console.log("Table: " + tableName);
    commitCallback(eventCtx);

  // Information Analyzer-generated failed records...
  } else if (infosphereEvent.applicationType === "Information Analyzer") {

    let ruleName = infosphereEvent.exceptionSummaryName;
    console.log("Rule : " + ruleName);
    iarest.getRuleExecutionResults(infosphereEvent.projectName, ruleName, false, function(err, aStats) {
      handleError("retrieving rule execution results", err);
      console.log(JSON.stringify(aStats));
      iarest.getRuleExecutionFailedRecordsFromLastRun(infosphereEvent.projectName, ruleName, 100, function(errRecords, aRecords, colMap) {
        handleError("retrieving detailed failed records", errRecords);
        console.log(JSON.stringify(aRecords));
        console.log(JSON.stringify(colMap));
        commitCallback(eventCtx);
      });
    });

  // Unknown where the failed records came from (not DataStage or Information Analyzer)...
  } else {
    console.log("Unhandled application type: " + infosphereEvent.applicationType);
    commitCallback(eventCtx);
  }

}
