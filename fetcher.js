'use strict';

const parse = require('./lib/parse.js');
const request = require('./lib/request.js');
const cwput = require('./lib/cwput.js');
const queue = require('./lib/queue.js');
const write = require('./lib/write.js');
const userCounts = require('./lib/userCounts.js');
const overallCounts = require('./lib/overallCounts.js');

exports.handler = (event, context, callback) => {
    const state = process.env.ReplicationPath + 'minute/state.txt';
    request.get(state)
        .then(parse.state)
        .then(getChanges)
        .then(parseChanges)
        .then(cwput.overallMetrics)
        .then(convertUserCounts)
        .then(convertOverallCounts)
        .then(context => write.fetcherStats(context.state.sequenceNumber, context.stats))
        .then(keys => queue.minuteAggregation(keys, true))
        .then(() => callback(null, null))
        .catch(callback);
};

function getChanges(context) {
    return new Promise((resolve, reject) => {
        request.changes(context.state.changeUrl)
            .then(readStream => {
                context.changes = readStream;
                resolve(context);
            })
            .catch(reject);
    });
}

function parseChanges(context) {
    return new Promise((resolve, reject) => {
        parse.changes(context.changes)
            .then(stats => {
                context.stats = stats;
                resolve(context);
            })
            .catch(reject);
    });
}

function convertUserCounts(context) {
    context.stats = userCounts.toCSV(context.stats);
    return context;
}

function convertOverallCounts(context) {
    context.stats = overallCounts.toCSV(context.stats);
    return context;
}
