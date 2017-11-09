'use strict';

const test = require('tape');
const trigger = require('../lib/trigger.js');
const AWS = require('aws-sdk-mock');

test('queue rollups', (t) => {
    AWS.mock('SQS', 'sendMessageBatch', (params, callback) => {
        var response = {
            ResponseMetadata: {RequestId: 'smthng'},
            Successful: [
                {
                    Id: '841a330440a5003af482543f484fa012941dfe57',
                    MessageId: 'something',
                    MD5OfMessageBody: 'ff714be138abc60e467d5f10e51c3eac'
                }, {
                    Id: '32d16f3fac5cb128748ec1b9aae89e2c72390b00',
                    MessageId: 'something2',
                    MD5OfMessageBody: '1d8ced801a3bfe4e13f62cd293ade9d0'
                }
            ],
            Failed: []
        };
        callback(null, response);
    });

    process.env.RollupQueue = 'https://sqs.us-west-2.amazonaws.com/732737962182/testing';

    const entries = [{
        datetime: 'yo',
        type: 'hour'
    }, {
        datetime: 'yo',
        type: 'day'
    }];

    trigger.rollups(entries)
        .then((response) => {
            t.equal(response.Successful.length, 2);
        })
        .then(AWS.restore)
        .then(t.end)
        .catch(console.log);
});
