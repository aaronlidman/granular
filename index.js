'use strict';

const config = require('./config.json');
const parse = require('./lib/parse.js');
const request = require('./lib/request.js');

module.exports.handler = function () {
    console.log(process.env);

    request.get(config.base_url + config.replication_dir + 'state.txt')
        .then(parse.state)
        .then(request.getStream)
        .then(parse.change)
        .catch((err) => console.log(err));
};
