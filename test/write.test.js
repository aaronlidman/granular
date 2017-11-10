'use strict';

const AWS = require('aws-sdk-mock');
const test = require('tape');
const zlib = require('zlib');

const write = require('../lib/write.js');

test('minutelyStats', (t) => {
    AWS.mock('DynamoDB', 'updateItem', function (params, callback) {
        var fileObj = JSON.parse(zlib.gunzipSync(params.ExpressionAttributeValues[':stats'].B));
        t.deepEqual(fileObj, {test: {c_node: 22, m_node: 17}}, 'file contents as expected');

        if (params.Key === (
            '2017-10-13T15:20-002669949' || '2017-10-13T15:19-002669949')) {
            t.ok(params.Key, 'keys are as expected');
        } else {
            t.error();
        }

        callback(null, {});
    });

    process.env.AWS_ACCESS_KEY_ID = null;
    process.env.AWS_SECRET_ACCESS_KEY = null;

    process.env.Bucket = 'bucket';
    process.env.Environment = 'environment';
    process.env.OutputPrefix = 'stack/';

    let promises = [];

    // plain old write
    promises.push(write.minutelyStats({
        stats: {'2017-10-13T15:20:00Z': {'test': {c_node: 22, m_node: 17}}},
        state: {sequenceNumber: '002669949'}
    }).then(t.ok).catch(t.error));

    // catch missing sequence
    promises.push(write.minutelyStats({
        stats: {'2017-10-13T15:20:00Z': {'test': {c_node: 22, m_node: 17}}},
    }).catch((err) => {
        t.equal(err, 'missing sequenceNumber', 'successfully errors on missing sequenceNumber');
    }));

    // write multiple files
    promises.push(write.minutelyStats({
        stats: {
            '2017-10-13T15:20:00Z': {'test': {c_node: 22, m_node: 17}},
            '2017-10-13T15:19:00Z': {'test': {c_node: 22, m_node: 17}}
        },
        state: {sequenceNumber: '002669949'}
    }).then((data) => {
        t.deepEqual(data,
            [{}, {}],
            'successfully wrote 2 files');
    }).catch(t.error));

    Promise.all(promises)
        .then(() => { AWS.restore(); })
        .then(t.end)
        .catch((error) => {
            t.error(error);
            t.end();
        });
});
