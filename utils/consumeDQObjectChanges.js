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
 * @file Builds a fully-detailed object (including all related information) based on any changes to DQ-related objects
 * @license Apache-2.0
 * @requires ibm-igc-rest
 * @requires ibm-iis-kafka
 * @requires ibm-iis-commons
 * @requires yargs
 * @example
 * // monitors any DQ-related object changes on hostname, and constructs fully-detailed objects whenever a DQ-related object changes
 * ./consumeDQObjectChanges.js -d hostname:9445 -z hostname:52181
 */

const igcrest = require('ibm-igc-rest');
const commons = require('ibm-iis-commons');
const iiskafka = require('../');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
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
const host_port = argv.domain.split(":");
const restConnect = new commons.RestConnection(argv.deploymentUser, argv.deploymentUserPassword, host_port[0], host_port[1]);
igcrest.setConnection(restConnect);

const infosphereEventEmitter = new iiskafka.InfosphereEventEmitter(argv.zookeeper, 'dq-object-handler', false);

infosphereEventEmitter.on('IGC_BUSINESSRULE_EVENT', processInfoGovernanceRule);
infosphereEventEmitter.on('IA_DATARULE_CREATED_EVENT', processNewDataRule);
infosphereEventEmitter.on('IA_DATARULE_MODIFIED_EVENT', processChangedDataRule);
infosphereEventEmitter.on('IGC_STEWARD_EVENT', processDataSteward);
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

function processInfoGovernanceRule(infosphereEvent, eventCtx, commitCallback) {
  console.log("Processing an Information Governance Rule...");
  const rid = infosphereEvent.ASSET_RID;
  const action = infosphereEvent.ACTION;
  const name = infosphereEvent.ASSET_NAME;
  const ctx = infosphereEvent.ASSET_CONTEXT;
  console.log(JSON.stringify(infosphereEvent));
  commitCallback(eventCtx);
}

function processNewDataRule(infosphereEvent, eventCtx, commitCallback) {
  console.log("Processing a new Data Rule...");
  const projectRid = infosphereEvent.projectRid;
  const tamRid = infosphereEvent.tamRid; // TODO: this cannot be resolved to anything by REST API ("Asset not supported")
  const ruleName = ""; // TODO: this is not provided in the Kafka message...
  console.log(JSON.stringify(infosphereEvent));
  //getDataRuleDetails(ruleName);
  commitCallback(eventCtx);
}

// TODO: receives the same information as a NEW event (above) -- just call out to the same generic logic
function processChangedDataRule(infosphereEvent, eventCtx, commitCallback) {
  console.log("Processing a modified Data Rule...");
  const projectRid = infosphereEvent.projectRid;
  const tamRid = infosphereEvent.tamRid; // TODO: this cannot be resolved to anything by REST API ("Asset not supported")
  const ruleName = ""; // TODO: this is not provided in the Kafka message...
  console.log(JSON.stringify(infosphereEvent));
  //getDataRuleDetails(ruleName);
  commitCallback(eventCtx);
}

// TODO: filter down these events to only those that are DQ-relevant (the event itself will be ANY steward event, not only those related to DQ objects)
function processDataSteward(infosphereEvent, eventCtx, commitCallback) {
  console.log("Processing a Data Steward...");
  const rid = infosphereEvent.ASSET_RID;          // the RID of the object that was assigned the Steward
  const action = infosphereEvent.ACTION;          // e.g. ASSIGNED_RELATIONSHIP
  const stewardName = infosphereEvent.ASSET_NAME; // Note: this is the full name, not username
  const ctx = infosphereEvent.ASSET_CONTEXT;
  console.log(JSON.stringify(infosphereEvent));
  commitCallback(eventCtx);
}

function getDataRuleDetails(ruleName) {
  
  const iaDataRuleQ = {
    "pageSize": "10000",
    "properties": [ "name", "implements_rules", "implements_rules.referencing_policies", "implements_rules.governs_assets", "implemented_bindings", "implemented_bindings.assigned_to_terms", "implemented_bindings.assigned_to_terms.stewards" ],
    "types": [ "data_rule" ],
    "operator": "and",
    "conditions":
    [
      {
        "property": "name",
        "operator": "=",
        "value": ruleName
      }
    ]
  };

  igcrest.search(iaDataRuleQ, function (err, resSearch) {

    if (resSearch.items.length === 0) {
      console.warn("WARN: Did not find any Data Rules with the name '" + ruleName + "'.");
    } else {
  
      for (let r = 0; r < resSearch.items.length; r++) {
  
        const rule = resSearch.items[r];
        const ruleName = rule._name;
        const policyDetails = rule["implements_rules.referencing_policies"].items;
        const infoGovRuleDetails = rule.implements_rules.items;
        const termDetails = rule["implemented_bindings.assigned_to_terms"].items;
        const governedAssets = rule["implements_rules.governs_assets"].items;
        const bindingDetails = rule.implemented_bindings.items;
  
        if (policyDetails.length === 0 || infoGovRuleDetails.length === 0 || bindingDetails.length === 0) {
          console.warn("WARN: Rule '" + ruleName + "' is missing one or more required relationships.");
        } else {
          const dqDimension = policyDetails[0]._name;
          const infoGovRuleName = infoGovRuleDetails[0]._name;
          const aColNames = [];
          const aColRIDs = [];
          for (let i = 0; i < bindingDetails.length; i++) {
            aColNames.push(bindingDetails[i]._name);
            aColRIDs.push(bindingDetails[i]._id);
          }

          const aTerms = [];
          const aStewards = [];
          let iFoundTerms = 0;
          const iProcessedTerms = 0;
          if (termDetails.length === 0) {
            for (let i = 0; i < governedAssets.length; i++) {
              if (governedAssets[i]._type === "term") {
                iFoundTerms++;
                aTerms.push(governedAssets[i]._name);
                const objDetails = {
                  'ruleName': ruleName,
                  'infoGovRuleName': infoGovRuleName,
                  'dqDimension': dqDimension,
                  'aColNames': aColNames,
                  'aColRIDs': aColRIDs,
                  'aTerms': aTerms
                };
                getDataOwners("term", governedAssets[i]._id, objDetails, iProcessedTerms, iFoundTerms, aStewards, processAllCollectedDataForRule);
              }
            }
          } else {
            for (let i = 0; i < termDetails.length; i++) {
              iFoundTerms++;
              aTerms.push(termDetails[i]._name);
              const objDetails = {
                'ruleName': ruleName,
                'infoGovRuleName': infoGovRuleName,
                'dqDimension': dqDimension,
                'aColNames': aColNames,
                'aColRIDs': aColRIDs,
                'aTerms': aTerms
              };
              getDataOwners("term", termDetails[i]._id, objDetails, iProcessedTerms, iFoundTerms, aStewards, processAllCollectedDataForRule);
            }
          }
        }
  
      }
  
    }
  
  });

}

function getDataOwners(type, rid, passthru, iProcessed, iFound, aStewardsSoFar, callback) {
  igcrest.getAssetPropertiesById(rid, type, ["stewards"], 100, true, function(err, resAsset) {
    let errAsset = err;
    const aStewardsForAsset = [];
    if (resAsset === undefined || (errAsset !== null && errAsset.startsWith("WARN: No assets found"))) {
      errAsset = "Unable to find a " + type + " with RID = " + rid;
    } else {
      const stewards = resAsset.stewards.items;
      for (let j = 0; j < stewards.length; j++) {
        aStewardsForAsset.push(stewards[j]._name);
      }
    }
    return callback(errAsset, aStewardsForAsset, iProcessed, iFound, passthru, aStewardsSoFar);
  });
}

function processAllCollectedDataForRule(err, aStewardsForOneObject, iProcessed, iFound, passthru, aStewards) {
  handleError("processing data collected for rule", err);
  iProcessed++;
  aStewards.push.apply(aStewards, aStewardsForOneObject);
  if (iProcessed === iFound) {
    console.log("Found the following for rule '" + passthru.ruleName + "':");
    console.log("  - Info gov rule   = " + passthru.infoGovRuleName);
    console.log("  - DQ dimension    = " + passthru.dqDimension);
    console.log("  - Bound column    = " + passthru.aColNames + " (" + passthru.aColRIDs + ")");
    console.log("  - Related term(s) = " + passthru.aTerms);
    console.log("  - Data owner(s)   = " + aStewards);
  }
}
