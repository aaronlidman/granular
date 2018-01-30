'use strict';

const isotrunc = require('./lib/isotrunc');

exports.handler = (event, context, callback) => {
    console.log('event', event);
    console.log('context', context);

    const response = {
        statusCode: 200,
        headers: {},
        isBase64Encoded: false
    };

    // no paths or methods, just all GETs through the querystring

    let time = event.queryStringParameters.time;

    if (!time) {
        response.statusCode = 400;
        return callback(null, response);
    }

    // convert a timestamp into parent and sequence
    time = time.slice(0, 16);
    response.body = JSON.stringify(isotrunc(time).parts());

    callback(null, response);
};
