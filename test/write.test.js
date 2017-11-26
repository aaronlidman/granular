'use strict';

const AWS = require('aws-sdk-mock');
const test = require('tape');
const zlib = require('zlib');

const write = require('../lib/write.js');

test('minutelyStats', (t) => {
    AWS.mock('DynamoDB', 'updateItem', function (params, callback) {
        console.log('fucker');

        var fileObj = JSON.parse(zlib.gunzipSync(params.ExpressionAttributeValues[':userCounts'].B));
        t.deepEqual(fileObj, {test: {create_node: 22, modify_node: 17}}, 'file contents as expected');

        if (params.Key.minute.S === '2017-10-13T15:19' ||
            params.Key.minute.S === '2017-10-13T15:20') {
            t.ok(true);
        } else {
            t.error(params.Key.minute.S, 'key not expected');
        }

        t.equal(params.Key.sequence.S, '002669949');

        callback(null, {});
    });

    process.env.AWS_ACCESS_KEY_ID = null;
    process.env.AWS_SECRET_ACCESS_KEY = null;

    process.env.Bucket = 'bucket';
    process.env.Environment = 'environment';
    process.env.OutputPrefix = 'stack/';
    process.env.SequenceTable = 'sequencess';

    let promises = [];

    // plain old write
    promises.push(write.minutelyStats({
        stats: {
            '2017-10-13T15:20:00Z': {
                'userCounts': {'test': {create_node: 22, modify_node: 17}},
                'overallCounts': {create_node: 22, modify_node: 17}
            }
        },
        state: {sequenceNumber: '002669949'}
    }).then(t.ok).catch(t.error));

    // catch missing sequence
    promises.push(write.minutelyStats({
        stats: {
            '2017-10-13T15:20:00Z': {
                'userCounts': {'test': {create_node: 22, modify_node: 17}},
                'overallCounts': {create_node: 22, modify_node: 17}
            }
        },
    }).catch((err) => {
        t.equal(err, 'missing sequenceNumber', 'successfully errors on missing sequenceNumber');
    }));

    // write multiple files
    promises.push(write.minutelyStats({
        stats: {
            '2017-10-13T15:20:00Z': {
                'userCounts': {'test': {create_node: 22, modify_node: 17}},
                'overallCounts': {create_node: 22, modify_node: 17}
            },
            '2017-10-13T15:19:00Z': {
                'userCounts': {'test': {create_node: 22, modify_node: 17}},
                'overallCounts': {create_node: 22, modify_node: 17}
            }
        },
        state: {sequenceNumber: '002669949'}
    }).then((data) => {
        t.deepEqual(data,
            [{}, {}],
            'successfully wrote 2 files');
    }).catch(t.error));

    Promise.all(promises)
        .then(() => AWS.restore())
        .then(t.end)
        .catch(error => {
            t.error(error);
            t.end();
        });
});
