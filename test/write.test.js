'use strict';

const AWS = require('aws-sdk-mock');
const test = require('tape');
const zlib = require('zlib');

const write = require('../lib/write.js');

test('minutelyStats', (t) => {
    AWS.mock('S3', 'putObject', function (params, callback) {
        var testUser = zlib.gunzipSync(params.Body).toString().split('\n')[1];
        t.equal(testUser, 'test,22,17,0,0,0,0,0,0,0');
        if (params.Key === (
            'stack/environment/raw-stats/2017-10-13T15:20-002669949.csv.gz' ||
            'stack/environment/raw-stats/2017-10-13T15:19-002669949.csv.gz')) {
            t.ok(params.Key);
        }
        callback(null, 'successfully putObject');
    });

    AWS.mock('CloudWatch', 'putMetricData', function (params, callback) {
        t.equal(params.MetricData[0].MetricName, 'files_written');
        callback(null, true);
    });

    process.env.AWS_ACCESS_KEY_ID = null;
    process.env.AWS_SECRET_ACCESS_KEY = null;

    process.env.Bucket = 'bucket';
    process.env.Environment = 'environment';
    process.env.OutputPrefix = 'stack/';

    // plain old write
    write.minutelyStats({
        stats: {'2017-10-13T15:20:00Z': {'test': {cnode: 22, mnode: 17}}},
        state: {sequenceNumber: '002669949'}
    }).then(t.ok).catch(t.error);

    // catch missing sequence
    write.minutelyStats({
        stats: {'2017-10-13T15:20:00Z': {'test': {cnode: 22, mnode: 17}}},
    }).catch((err) => {
        t.equal(err, 'missing sequenceNumber');
    });

    // write multiple files
    write.minutelyStats({
        stats: {
            '2017-10-13T15:20:00Z': {'test': {cnode: 22, mnode: 17}},
            '2017-10-13T15:19:00Z': {'test': {cnode: 22, mnode: 17}}
        },
        state: {sequenceNumber: '002669949'}
    }).then((data) => {
        t.deepEqual(data, ['successfully putObject', 'successfully putObject']);
    }).catch(t.error);

    AWS.restore();

    t.end();
});
