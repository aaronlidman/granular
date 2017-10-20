'use strict';

const parse = require('./lib/parse.js');
const request = require('./lib/request.js');
const cwput = require('./lib/cwput.js');
const write = require('./lib/write.js');

module.exports.handler = function () {
    request.get(process.env.ReplicationPath + 'minute/state.txt')
        .then(parse.state)
        .then(request.changes)
        .then(parse.changes)
        .then(cwput.overallMetrics)
        .then(write.overallFile)
        .then((data) => console.log(data))
        .catch((err) => console.log(err));
};
