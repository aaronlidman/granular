'use strict';

const test = require('tape');
const AWS = require('aws-sdk-mock');
const queue = require('../lib/queue.js');

AWS.mock('SQS', 'sendMessage', (params, callback) => {
    callback(null, params);
});

AWS.mock('SQS', 'receiveMessage', (params, callback) => {
    if (params.QueueUrl === 'someQueue') {
        callback(null, {
            Messages: [{
                MessageId: 'messageid-123',
                ReceiptHandle: 'receiptHandle-123',
                MD5OfBody: 'md5-123',
                Body: '{"worker":"aggregator","jobType":"day","key":"2017-12-10"}',
                Attributes: {
                    SentTimestamp: parseInt(+new Date() / 1000)
                }
            }]
        });
    }

    if (params.QueueUrl === 'no-messages') {
        callback(null, {Messages: []});
    }
});

AWS.mock('SQS', 'deleteMessage', (params, callback) => {
    callback(null, params);
});

test('queue.sendMessage', t => {
    queue.sendMessage('idk', {'something': true})
        .then(result => {
            t.equal(result.QueueUrl, 'idk', 'QueueUrl is set');
            t.equal(result.MessageBody, '{"something":true}', 'MessageBody is as expected');
        })
        .then(t.end)
        .catch(t.error);
});

test('queue.sendMessage catch error', t => {
    queue.sendMessage()
        .then(t.error)
        .catch(err => {
            t.ok(err, 'surfaced error');
        })
        .then(t.end);
});

test('queue.getMessage', t => {
    queue.receiveMessage({QueueUrl: 'someQueue'})
        .then(result => {
            t.equal(result.Body, '{"worker":"aggregator","jobType":"day","key":"2017-12-10"}');
            t.equal(result.ReceiptHandle, 'receiptHandle-123');
        })
        .then(t.end)
        .catch(t.error);
});

test('queue.getMessage no messages', t => {
    queue.receiveMessage({QueueUrl: 'no-messages'})
        .then(() => {
            t.error('should not be results, should error');
        })
        .catch((err) => {
            t.ok(err, 'surfaced error');
        })
        .then(t.end);
});

test('queue.deleteMessage', t => {
    queue.deleteMessage({QueueUrl: 'delete-message', ReceiptHandle: 'some-string'}, 'md5')
        .then(t.ok)
        .catch(t.error)
        .then(t.end);
});
