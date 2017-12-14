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

AWS.mock('SQS', 'changeMessageVisibility', (params, callback) => {
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
        .then(() => {
            t.ok(true, 'message deleted');
            t.true(queue.skipList.md5, 'message reminants listed in skipList');
        })
        .catch(t.error)
        .then(t.end);
});

test('queue.hideMessageForFive', t => {
    queue.hideMessageForFive({QueueUrl: 'delete-message', ReceiptHandle: 'some-string'})
        .then(() => {
            t.ok(true, 'message hidden');
        })
        .catch(t.error)
        .then(t.end);
});

test('queue.minuteAggregation', t => {
    const keys = [
        '2017-01-02T03:04:05.678Z',
        '2017-01-02T03:04:06.678Z',
        '2017-01-02T03:05:05.678Z',
        '2017-01-02T08:04:05.678Z'
    ];

    process.env.fastQueue = 'something-fun';

    queue.minuteAggregation(keys, false)
        .then(results => {
            const expected = [{
                QueueUrl: 'something-fun',
                MessageBody: '{"worker":"aggregator","jobType":"minute","key":"2017-01-02T03:04"}'
            }, {
                QueueUrl: 'something-fun',
                MessageBody: '{"worker":"aggregator","jobType":"minute","key":"2017-01-02T03:04"}'
            }, {
                QueueUrl: 'something-fun',
                MessageBody: '{"worker":"aggregator","jobType":"minute","key":"2017-01-02T03:05"}'
            }, {
                QueueUrl: 'something-fun',
                MessageBody: '{"worker":"aggregator","jobType":"minute","key":"2017-01-02T08:04"}'
            }];
            t.deepEqual(results, expected);
        })
        .catch(t.error)
        .then(t.end);
});

test('queue.minuteAggregation with propogation', t => {
    const keys = [
        '2017-01-02T03:04:05.678Z',
        '2017-01-02T03:04:06.678Z',
        '2017-01-02T03:05:05.678Z',
        '2017-01-02T08:04:05.678Z'
    ];

    process.env.fastQueue = 'something-fun';
    process.env.slowQueue = 'something-less-fun';

    queue.minuteAggregation(keys, true)
        .then(results => {
            const expected = [{
                QueueUrl: 'something-fun',
                MessageBody: '{"worker":"aggregator","jobType":"minute","key":"2017-01-02T03:04"}'
            },
            {
                QueueUrl: 'something-fun',
                MessageBody: '{"worker":"aggregator","jobType":"minute","key":"2017-01-02T03:04"}'
            },
            {
                QueueUrl: 'something-fun',
                MessageBody: '{"worker":"aggregator","jobType":"minute","key":"2017-01-02T03:05"}'
            },
            {
                QueueUrl: 'something-fun',
                MessageBody: '{"worker":"aggregator","jobType":"minute","key":"2017-01-02T08:04"}'
            },
            [{
                    QueueUrl: 'something-fun',
                    MessageBody: '{"worker":"aggregator","jobType":"hour","key":"2017-01-02T03"}'
                },
                {
                    QueueUrl: 'something-fun',
                    MessageBody: '{"worker":"aggregator","jobType":"hour","key":"2017-01-02T08"}'
                }
            ],
            [{
                QueueUrl: 'something-less-fun',
                MessageBody: '{"worker":"aggregator","jobType":"day","key":"2017-01-02"}'
            }]
            ];

            // this output isn't the prettiest but doesn't especially matter
            // it's only an indication of things actually happening

            // a better way would be to actually access the results from the queue
            // until then, this will do

            t.deepEqual(results, expected);
        })
        .catch(t.error)
        .then(t.end);
});
