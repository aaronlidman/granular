'use strict';

const parse = require('./lib/parse.js');
const request = require('./lib/request.js');
const cwput = require('./lib/cwput.js');
const write = require('./lib/write.js');
const trigger = require('./lib/trigger.js');

module.exports.handler = function () {
    request.get(process.env.ReplicationPath + 'minute/state.txt')
        .then(parse.state)
        .then(request.changes)
        .then(parse.changes)
        .then(cwput.overallMetrics)
        .then(write.minutelyStats)
        .then(trigger.rollups)
        .then(console.log)
        .catch(err => { throw new Error(err); });
};
