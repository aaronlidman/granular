'use strict';

const isotrunc = require('./lib/isotrunc');
const request = require('./lib/request');

exports.handler = (event, context, callback) => {
    // no paths or methods, just all GETs and querystrings
    const response = {
        statusCode: 200,
        headers: {},
        isBase64Encoded: false
    };

    let time = (event.queryStringParameters || {}).time;
    if (!time) return callback(null, errorOut(response, 'must specify a time, eg ?time=2018-01-01T01:01'));

    // maximum of minute resolution
    if (time.length > 16) time = time.slice(0, 16);
    const parts = isotrunc(time).parts();

    request.countItems(parts.parent, parts.sequence)
        .then(result => {
            // ideally api-gateway supports streams in the future
            response.body = JSON.stringify(result);
            return callback(null, response);
        })
        .catch(err => {
            if (err.name === 'NoItems') {
                response.body = '[]';
                callback(null, response);
            } else {
                callback(errorOut(response, JSON.stringify(err)));
            }
        });
};

function errorOut(response, message) {
    response.statusCode = 400;
    response.body = message;
    return response;
}
