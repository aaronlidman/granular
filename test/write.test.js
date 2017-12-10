'use strict';

const AWS = require('aws-sdk-mock');
const test = require('tape');
const zlib = require('zlib');

const write = require('../lib/write.js');

test('minutelyStats', (t) => {
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

    process.env.AWS_ACCESS_KEY_ID = null;
    process.env.AWS_SECRET_ACCESS_KEY = null;

    process.env.Bucket = 'bucket';
    process.env.Environment = 'environment';
    process.env.OutputPrefix = 'stack/';
    process.env.MainTable = 'maintableee';

    let promises = [];

    // plain old write
    promises.push(write.fetcherStats({
        stats: {
            '2017-10-13T15:20:00Z': {
                'userCounts': 'test,22,17,0',
                'overallCounts': '22,17,0'
            }
        },
        state: {sequenceNumber: '002669949'}
    }).then(t.ok).catch(t.error));

    // catch missing sequence
    promises.push(write.fetcherStats({
        stats: {
            '2017-10-13T15:20:00Z': {
                'userCounts': 'test,22,17,0',
                'overallCounts': '22,17,0'
            }
        },
    }).catch((err) => {
        t.equal(err, 'missing sequenceNumber', 'successfully errors on missing sequenceNumber');
    }));

    //write multiple files
    promises.push(write.fetcherStats({
        stats: {
            '2017-10-13T15:20:00Z': {
                'userCounts': 'test,22,17,0',
                'overallCounts': '22,17,0'
            },
            '2017-10-13T15:19:00Z': {
                'userCounts': 'test,22,17,0',
                'overallCounts': '22,17,0'
            }
        },
        state: {sequenceNumber: '002669949'}
    }).then((data) => {
        t.deepEqual(data,
            ['2017-10-13T15:20', '2017-10-13T15:19'],
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
