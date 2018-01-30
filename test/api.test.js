'use strict';

const test = require('tape');
const api = require('../api.js');
const isotrunc = require('../lib/isotrunc.js');

test('400 response', t => {
    api.handler({}, {}, (error, response) => {
        t.equal(error, null);
        t.deepEqual(response, {
            statusCode: 400,
            headers: {},
            isBase64Encoded: false
        });
        t.end();
    });
});

test('200 response', t => {
    const time = new Date().toISOString();

    const event = {
        queryStringParameters: {
            time: time
        }
    };

    api.handler(event, {}, (error, response) => {
        t.equal(error, null);
        t.deepEqual(response, {
            statusCode: 200,
            headers: {},
            isBase64Encoded: false,
            body: JSON.stringify(isotrunc(time).parts())
        });
        t.end();
    });

});
