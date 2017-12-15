'use strict';

const AWS = require('aws-sdk-mock');
const test = require('tape');
const zlib = require('zlib');

const write = require('../lib/write.js');

test('write.fetcherStats', t => {
    AWS.mock('DynamoDB', 'updateItem', function (params, callback) {
        var fileObj = zlib.gunzipSync(params.ExpressionAttributeValues[':userCounts'].B);
        t.deepEqual(fileObj.toString(), 'test,22,17,0', 'file contents as expected');

        if (params.Key.parent.S === '2017-10-13T15:19' ||
            params.Key.parent.S === '2017-10-13T15:20') {
            t.ok(true);
        } else {
            t.error(params.Key.parent.S, 'key not expected');
        }

        t.equal(params.Key.sequence.N, '002669949');

        callback(null, {});
    });

    let promises = [];

    // plain old write
    const sequence = '002669949';
    let stats = {
        '2017-10-13T15:20:00Z': {
            'userCounts': 'test,22,17,0',
            'overallCounts': '22,17,0'
        }
    };
    promises.push(write.fetcherStats(sequence, stats)
        .then(t.ok)
        .catch(t.error));

    // catch missing sequence
    stats = {
        '2017-10-13T15:20:00Z': {
            'userCounts': 'test,22,17,0',
            'overallCounts': '22,17,0'
        }
    };
    promises.push(write.fetcherStats(undefined, stats).catch((err) => {
        t.equal(err.message, 'missing sequenceNumber', 'successfully errors on missing sequenceNumber');
    }));

    //write multiple files
    stats = {
        '2017-10-13T15:20:00Z': {
            'userCounts': 'test,22,17,0',
            'overallCounts': '22,17,0'
        },
        '2017-10-13T15:19:00Z': {
            'userCounts': 'test,22,17,0',
            'overallCounts': '22,17,0'
        }
    };
    promises.push(write.fetcherStats('002669949', stats)
        .then(data => {
            t.deepEqual(data, ['2017-10-13T15:20', '2017-10-13T15:19'], 'successfully wrote 2 files');
        })
        .catch(t.error));

    Promise.all(promises)
        .then(() => AWS.restore())
        .then(t.end)
        .catch(error => {
            t.error(error);
            t.end();
        });
});

test('write.aggregate', t => {
    AWS.mock('DynamoDB', 'updateItem', function (params, callback) {
        callback(null, params);
    });

    process.env.MainTable = 'maintableee';

    const key = {
        parent: 'parent',
        sequence: '123'
    };

    const data = {
        something: 'here',
        somethingElse: 'here'
    };

    write.aggregate(key, data)
        .then(results => {
            t.deepEqual(results, {
                TableName: 'maintableee',
                Key: {parent: {S: 'parent'}, sequence: {N: '123'}},
                UpdateExpression: 'SET #SOMETHING = :something, #SOMETHINGELSE = :somethingElse',
                ExpressionAttributeNames: {'#SOMETHING': 'something', '#SOMETHINGELSE': 'somethingElse'},
                ExpressionAttributeValues: {
                    ':something': {B: zlib.gzipSync(data.something)},
                    ':somethingElse': {B: zlib.gzipSync(data.somethingElse)}
                }
            });
        })
        .catch(t.error)
        .then(t.end);
});
