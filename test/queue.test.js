'use strict';

const test = require('tape');
const AWS = require('aws-sdk-mock');
const queue = require('../lib/queue.js');

AWS.mock('SQS', 'sendMessage', (params, callback) => {
    callback(null, params);
});

test('queue.putMessage', t => {
    queue.putMessage('idk', {'something': true})
        .then(result => {
            t.equal(result.QueueUrl, 'idk', 'QueueUrl is set');
            t.equal(result.MessageBody, '{"something":true}', 'MessageBody is as expected');
        })
        .then(t.end)
        .catch(t.error);
});

test('catch error', t => {
    queue.putMessage()
        .then(t.error)
        .catch(err => {
            t.ok(err, 'surfaced error');
        })
        .then(t.end);
});
