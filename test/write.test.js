'use strict';

const AWS = require('aws-sdk-mock');
const test = require('tape');
const zlib = require('zlib');

const write = require('../lib/write.js');

test('minutelyStats', (t) => {
    AWS.mock('S3', 'putObject', function (params, callback) {
        let time = params.Key.split('/').slice(-1)[0].slice(0, 16) + ':00Z';
        let testUser = zlib.gunzipSync(params.Body).toString().split('\n')[0];
        t.equal(testUser, time + ',test,22,17,0,0,0,0,0,0,0', 'file contents as expected');

        if (params.Key === (
            'stack/environment/raw-stats/2017-10-13T15:20-002669949.csv.gz' ||
            'stack/environment/raw-stats/2017-10-13T15:19-002669949.csv.gz')) {
            t.ok(params.Key, 'files written to the right place');
        } else {
            t.error();
        }

        callback(null, params.Key);
    });

    AWS.mock('CloudWatch', 'putMetricData', function (params, callback) {
        t.equal(params.MetricData[0].MetricName, 'files_written', 'files_written metric put');
        callback(null, true);
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
            [
                'stack/environment/raw-stats/minutes/2017-10-13T15:20-002669949.csv.gz',
                'stack/environment/raw-stats/minutes/2017-10-13T15:19-002669949.csv.gz'
            ],
            'successfully wrote multiple files');
    }).catch(t.error));

    Promise.all(promises)
        .then(AWS.restore)
        .then(t.end)
        .catch((error) => {
            t.error(error);
            t.end();
        });
});
