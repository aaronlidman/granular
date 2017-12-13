'use strict';

const test = require('tape');
const isotrunc = require('../lib/isotrunc.js');

['2017-01-02T03:04:05.678Z', new Date().toISOString()].forEach(example => {
    const year = isotrunc.to(example, 'year');
    const month = isotrunc.to(example, 'month');
    const day = isotrunc.to(example, 'day');
    const hour = isotrunc.to(example, 'hour');
    const minute = isotrunc.to(example, 'minute');

    test('isotrunc.to ' + example, t => {
        t.equal(year, example.slice(0, 4));
        t.equal(month, example.slice(0, 7));
        t.equal(day, example.slice(0, 10));
        t.equal(hour, example.slice(0, 13));
        t.equal(minute, example.slice(0, 16));
        t.end();
    });

    test('isotrunc.parent ' + example, t => {
        t.equal(isotrunc.parent(year), example.slice(0, 4));
        t.equal(isotrunc.parent(month), isotrunc.to(example, 'year'));
        t.equal(isotrunc.parent(day), isotrunc.to(example, 'month'));
        t.equal(isotrunc.parent(hour), isotrunc.to(example, 'day'));
        t.equal(isotrunc.parent(minute), isotrunc.to(example, 'hour'));
        t.end();
    });

    test('isotrunc.unit ' + example, t => {
        t.equal(isotrunc.unit(year), 'year');
        t.equal(isotrunc.unit(month), 'month');
        t.equal(isotrunc.unit(day), 'day');
        t.equal(isotrunc.unit(hour), 'hour');
        t.equal(isotrunc.unit(minute), 'minute');
        t.end();
    });
});
