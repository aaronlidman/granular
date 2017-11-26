'use strict';

const parse = require('./lib/parse.js');
const request = require('./lib/request.js');
const cwput = require('./lib/cwput.js');
const write = require('./lib/write.js');

exports.handler = () => {
    request.get(process.env.ReplicationPath + 'minute/state.txt')
        .then(parse.state)
        .then(request.changes)
        .then(parse.changes)
        .then(cwput.overallMetrics)
        .then(write.minutelyStats)
        .catch(err => { throw new Error(err); });
};
