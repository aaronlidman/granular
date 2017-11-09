'use strict';

const test = require('tape');
const rollup = require('../lib/rollup.js');
const AWS = require('aws-sdk-mock');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

test('roll up minute files', (t) => {
    AWS.mock('S3', 'listObjectsV2', (params, callback) => {
        callback(null, {
            'Contents': [
                {'Key': 'prefix/enviro/raw-stats/minutes/2017-01-01T00:00-002693192.csv.gz'},
                {'Key': 'prefix/enviro/raw-stats/minutes/2017-01-01T00:00-002693193.csv.gz'},
                {'Key': 'prefix/enviro/raw-stats/minutes/2017-01-01T00:01-002693193.csv.gz'},
                {'Key': 'prefix/enviro/raw-stats/minutes/2017-01-01T00:01-002693194.csv.gz'},
                {'Key': 'prefix/enviro/raw-stats/minutes/2017-01-01T00:02-002693194.csv.gz'}
            ]
        });
    });

    AWS.mock('S3', 'getObject', (params, callback) => {
        callback(null, {
            Body: fs.readFileSync(path.join(__dirname, './fixtures/2017-01-01T00-02-002693194.csv.gz'))
        });
    });

    AWS.mock('S3', 'putObject', (params, callback) => {
        t.equal(params.Key, 'prefix/enviro/raw-stats/hours/2017-01-01T00.csv.gz');
        t.equal(zlib.gunzipSync(params.Body).toString().split('\n').length, 86);
        callback(null, params.Key);
    });

    process.env.Bucket = 'and-shovel';
    process.env.OutputPrefix = 'prefix/';
    process.env.Environment = 'enviro';

    rollup('2017-01-01T00', 'hour')
        .then(() => {
            return;
        })
        .then(AWS.restore)
        .then(t.end)
        .catch((err) => {
            t.error(err);
            t.end();
        });
});
