'use strict';

const fs = require('fs');
const path = require('path');

const test = require('tape');
const nock = require('nock');
const request = require('../lib/request.js');

test('get', (t) => {
    const statePath = path.join(__dirname, './fixtures/state1.txt');
    const state = fs.readFileSync(statePath).toString();

    nock('http://planet.osm.org')
        .get('/replication/minute/state.txt')
        .reply(200, state);

    request.get('http://planet.osm.org/replication/minute/state.txt')
        .then((response) => {
            t.equal(response, state, 'request works');
        });

    request.get('http://aslkw.xyz')
        .catch((error) => {
            t.equal(error.code, 'ENOTFOUND', 'error catches correctly');
        });

    t.end();
});

test('getGzipStream', (t) => {
    const changePath = path.join(__dirname, './fixtures/change1.osc.gz');
    const changeFile = fs.createReadStream(changePath);

    nock('http://yoyo.go')
        .get('/change.osc.gz')
        .reply(changeFile);

    request.getGzipStream('http://yoyo.go/change.osc.gz')
        .then((response) => {
            t.ok(response.readable, 'got a readableStream as expected');

            // this catches on circle for some reason
            response.on('error', t.ok);
        });

    request.getGzipStream('http://yoyo.go/change.osc.gz')
        .then(() => {
            t.error('should not respond');
        })
        .catch((err) => {
            t.equal(err.status, 404, 'catches error correctly');
        });

    t.end();
});
