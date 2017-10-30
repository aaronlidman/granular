'use strict';

const fs = require('fs');
const zlib = require('zlib');
const path = require('path');

const test = require('tape');
const parse = require('../lib/parse.js');

process.env.ReplicationPath = 'http://planet.osm.org/replication/';

test('parse state file', (t) => {
    const stateFile = path.join(__dirname, './fixtures/state1.txt');
    parse.state(fs.readFileSync(stateFile).toString())
        .then((result) => {
            t.true(result.state, 'state property is present');
            t.true(result.state.changeUrl, 'changeUrl property is present');

            t.equal(result.state.sequenceNumber, '002669949',
                'sequenceNumber is as expected');
            t.equal(result.state.changeUrl,
                'http://planet.osm.org/replication/minute/002/669/949.osc.gz',
                'changeUrl is as expected');

            t.end();
        }).catch(t.error);
});

test('parse change file', (t) => {
    const changeFile = path.join(__dirname, './fixtures/change1.osc.gz');
    const readStream = fs.createReadStream(changeFile);
    const obj = {changes: readStream.pipe(zlib.createGunzip())};

    parse.changes(obj)
        .then((result) => {
            t.true(result.stats['2017-10-13T15:20:00Z']['_overall'], 'overall stats are present');
            t.true(result.stats['2017-10-13T15:21:00Z'].mavl, 'random user is present');
            t.true(result.stats['2017-10-13T15:20:00Z']['Chris McKay'], 'random user is present');

            t.deepEqual(result.stats['2017-10-13T15:20:00Z'].gloriaq, {c_node: 26, c_way: 26}, 'counts as expected');
            t.deepEqual(result.stats['2017-10-13T15:20:00Z'].vivekanandapai, {m_node: 9, c_node: 228, m_way: 12, c_way: 41},
                'counts as expected');
            t.deepEqual(result.stats['2017-10-13T15:20:00Z']['_overall'],
                {
                    m_node: 410, d_node: 187, c_node: 2335, m_way: 169, d_way: 7,
                    c_way: 282, m_relation: 4, d_relation: 8, c_relation: 5
                },
                'counts as expected');
            t.deepEqual(Object.keys(result.stats), ['2017-10-13T15:20:00Z', '2017-10-13T15:21:00Z'], 'timestamps are as expected');
            t.equal(result.state, undefined, 'state is not present');
            t.end();
        }).catch(t.error);
});
