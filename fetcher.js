'use strict';

const parse = require('./lib/parse.js');
const request = require('./lib/request.js');
const cwput = require('./lib/cwput.js');
const queue = require('./lib/queue.js');
const write = require('./lib/write.js');
const userCounts = require('./lib/userCounts.js');
const overallCounts = require('./lib/overallCounts.js');

exports.handler = (event, context, callback) => {
    request.get(process.env.ReplicationPath + 'minute/state.txt')
        .then(parse.state)
        .then(request.changes)
        .then(parse.changes)
        .then(cwput.overallMetrics)
        .then(userCounts.toCSV)
        .then(overallCounts.toCSV)
        .then(write.fetcherStats)
        .then(keys => queue.minuteAggregation(keys, true))
        .catch(callback);
};
