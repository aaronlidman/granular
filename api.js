'use strict';

const isotrunc = require('./lib/isotrunc');
const request = require('./lib/request');

exports.handler = (event, context, callback) => {
    console.log('event', event);
    console.log('context', context);

    const response = {
        statusCode: 200,
        headers: {},
        isBase64Encoded: false
    };

    // no paths or methods, just all GETs and querystrings

    let time = (event.queryStringParameters || {}).time;
    if (!time) return callback(errorOut(response, 'must specify a time, eg ?time=2018-01-01T01:01'));

    if (time.length > 16) time = time.slice(0, 16);
    const parts = isotrunc(time).parts();

    request.countItems(parts.parent, parts.sequence)
        .then(result => {
            // stream this eventually
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
