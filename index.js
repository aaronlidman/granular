'use strict';

const config = require('./config.json');
const parse = require('./lib/parse.js');
const request = require('./lib/request.js');
const cwput = require('./lib/cwput.js');
const write = require('./lib/write.js');

module.exports.handler = function () {
    let time;

    request.get(config.base_url + config.replication_dir + 'state.txt')
        .then(parse.state)
        .then((data) => {
            time = data.state.timestamp;
            return request.getGzipStream(data.changeUrl);
        })
        .then(parse.change)
        .then((stats) => {
            time = stats['_minute'];
            delete stats['_minute'];
            // carry this properly

            return cwput.overallMetrics(stats, time);
        })
        .then(write.overallFile)
        .then((data) => console.log(data))
        .catch((err) => console.log(err));
};
