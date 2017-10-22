'use strict';

const AWS = require('aws-sdk-mock');
const test = require('tape');
const zlib = require('zlib');

const write = require('../lib/write.js');

test('overallFile', (t) => {
    AWS.mock('S3', 'putObject', function (params, callback) {
        var testUser = zlib.gunzipSync(params.Body).toString().split('\n')[1];
        t.equal(testUser, 'test,22,17,0,0,0,0,0,0,0');
        t.equal(params.Key, 'yes/warm/minutes/2017-10-13T15:20.csv.gz');
        callback(null, 'successfully putObject');
    });

    process.env.AWS_ACCESS_KEY_ID = null;
    process.env.AWS_SECRET_ACCESS_KEY = null;

    process.env.Environment = 'warm';
    process.env.Bucket = 'shovel';
    process.env.OutputPrefix = 'yes/';

    write.overallFile({
        stats: {'test': {cnode: 22, mnode: 17}},
        time: '2017-10-13T15:20:00Z'
    })
        .then(t.ok)
        .catch(t.error);

    write.overallFile({
        stats: {'test': {cnode: 22, mnode: 17}}
    }).catch((error) => {
        t.equal(error, 'missing timestamp');
    });

    AWS.restore();

    t.end();
});
