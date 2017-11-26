'use strict';

const test = require('tape');
const AWS = require('aws-sdk-mock');
const queue = require('../lib/queue.js');

AWS.mock('SQS', 'sendMessage', (params, callback) => {
    callback(null, params);
});

test('queue.generic', t => {
    queue.generic('idk', {'something': true})
        .then(result => {
            t.equal(result.QueueUrl, 'idk', 'QueueUrl is set');
            t.equal(result.MessageBody, '{"something":true}', 'MessageBody is as expected');
        })
        .then(t.end)
        .catch(t.error);
});

test('queue.sequence', t => {
    process.env.perMinQueue = 'hey';

    queue.sequence('1553')
        .then(result => {
            t.equal(result.QueueUrl, process.env.perMinQueue, 'QueueUrl set by env var');
            t.equal(result.MessageBody, '{"worker":"aggregator","jobType":"sequence","key":"1553"}', 'MessageBody is as expected');
        })
        .then(t.end)
        .catch(t.error);
});

test('catch error', t => {
    queue.generic('idk')
        .then(t.error)
        .catch(err => {
            t.equal(err.code, 'MissingRequiredParameter', 'surfaced error correctly');
        })
        .then(t.end);
});
