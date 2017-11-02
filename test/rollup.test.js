'use strict';

const test = require('tape');
const rollup = require('../lib/rollup.js');

test('roll up minute files', (t) => {
    process.env.Bucket = 'aaronlidman-west-2';
    process.env.OutputPrefix = 'osm-dash/';
    process.env.Environment = 'staging';

    rollup('2017-11-02T03', 'hour')
        .then((data) => {
            console.log('done');
            console.log(data);
            t.end();
        })
        .catch((err) => {
            console.log('err', err);
            t.end();
        });
});
