'use strict';

const AWS = require('aws-sdk-mock');
const test = require('tape');
const zlib = require('zlib');

const write = require('../lib/write.js');

test('overallFile', (t) => {
    AWS.mock('S3', 'putObject', function (params, callback) {
        t.ok(params);

        var testUser = zlib.gunzipSync(params.Body).toString().split('\n')[1];
        t.equal(testUser, 'test,22,17,0,0,0,0,0,0,0');

        callback(null, 'successfully putObject');
    });

    process.env.AWS_ACCESS_KEY_ID = null;
    process.env.AWS_SECRET_ACCESS_KEY = null;

    process.env.Environment = 'warm';
    process.env.Bucket = 'shovel';
    process.env.OutputPrefix = 'yes/';

    write.overallFile({stats: {'test': {cnode: 22, mnode: 17}}})
        .then(t.ok)
        .catch(t.error);

    AWS.restore();

    t.end();
});
