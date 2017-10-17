'use strict';

const fs = require('fs');
const zlib = require('zlib');
const path = require('path');

const test = require('tape');
const parse = require('../lib/parse.js');

test('parse state file', (t) => {
    var stateFile = path.join(__dirname, './fixtures/state1.txt');
    parse.state(fs.readFileSync(stateFile).toString())
        .then((result) => {
            t.true(result.state, 'state property is present');
            t.true(result.changeUrl, 'changeUrl property is present');

            t.equal(result.state.sequenceNumber, '002669949',
                'sequenceNumber is as expected');
            t.equal(result.changeUrl,
                'http://planet.osm.org/replication/minute/002/669/949.osc.gz',
                'changeUrl is as expected');

            t.end();
        }).catch(t.error);
});

test('parse change file', (t) => {
    var changeFile = path.join(__dirname, './fixtures/change1.osc.gz');
    var readStream = fs.createReadStream(changeFile);

    parse.change(readStream.pipe(zlib.createGunzip()))
        .then((result) => {
            t.true(result['_overall'], 'overall stats are present');
            t.true(result.cb75, 'random user is present');
            t.true(result['Chris McKay'], 'random user is present');

            t.deepEqual(result.gloriaq, {cnode: 26, cway: 26}, 'counts as expected');
            t.deepEqual(result.vivekanandapai, {mnode: 9, cnode: 228, mway: 12, cway: 41},
                'counts as expected');
            t.deepEqual(result['_overall'],
                {
                    mnode: 411, dnode: 187, cnode: 2340, mway: 169, dway: 7,
                    cway: 282, mrelation: 4, drelation: 8, crelation: 5
                },
                'counts as expected');
            t.end();
        }).catch(t.error);
});
