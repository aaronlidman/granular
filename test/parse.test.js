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
        .then(result => {
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
        .then(result => {
            t.true(result.stats['2017-10-13T15:20'].overallCounts, 'overall stats are present');
            t.true(result.stats['2017-10-13T15:21'].userCounts.mavl, 'random user is present');
            t.true(result.stats['2017-10-13T15:20'].userCounts['Chris McKay'], 'random user is present');

            t.deepEqual(result.stats['2017-10-13T15:20'].userCounts.gloriaq, {create_node: 26, create_way: 26}, 'counts as expected');
            t.deepEqual(result.stats['2017-10-13T15:20'].userCounts.vivekanandapai, {modify_node: 9, create_node: 228, modify_way: 12, create_way: 41},
                'counts as expected');
            t.deepEqual(result.stats['2017-10-13T15:20'].overallCounts,
                {
                    modify_node: 410, delete_node: 187, create_node: 2335, modify_way: 169, delete_way: 7,
                    create_way: 282, modify_relation: 4, delete_relation: 8, create_relation: 5
                },
                'counts as expected');
            t.deepEqual(Object.keys(result.stats), ['2017-10-13T15:20', '2017-10-13T15:21'], 'timestamps are as expected');
            t.equal(result.state, undefined, 'state is not present');
            t.end();
        }).catch(t.error);
});
