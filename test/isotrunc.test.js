'use strict';

const test = require('tape');
const isotrunc = require('../lib/isotrunc.js');

const timeFixtures = {
    '2018': {
        'to': {
            'year': '2018',
            'month': undefined,
            'day': undefined
        },
        'unit': 'year',
        'parent': '2018',
        'sequence': undefined
    },
    '2018-10': {
        'to': {
            'year': '2018',
            'month': '2018-10',
            'day': undefined
        },
        'unit': 'month',
        'parent': '2018',
        'sequence': 10
    },
    '2018-10-20': {
        'to': {
            'year': '2018',
            'month': '2018-10',
            'day': '2018-10-20',
            'hour': undefined
        },
        'unit': 'day',
        'parent': '2018-10',
        'sequence': 20
    },
    '2018-10-20T12': {
        'to': {
            'year': '2018',
            'month': '2018-10',
            'day': '2018-10-20',
            'hour': '2018-10-20T12',
            'minute': undefined
        },
        'unit': 'hour',
        'parent': '2018-10-20',
        'sequence': 12
    },
    '2018-10-20T12:05': {
        'to': {
            'year': '2018',
            'month': '2018-10',
            'day': '2018-10-20',
            'hour': '2018-10-20T12',
            'minute': '2018-10-20T12:05'
        },
        'unit': 'minute',
        'parent': '2018-10-20T12',
        'sequence': 5
    },
    '2018-10-20T12:05:26': {
        'to': {
            'year': '2018',
            'month': '2018-10',
            'day': '2018-10-20',
            'hour': '2018-10-20T12',
            'minute': '2018-10-20T12:05'
        },
        'unit': 'minute',
        'parent': '2018-10-20T12',
        'sequence': 5
    },
    '2018-10-20T12:05:26.277Z': {
        'to': {
            'year': '2018',
            'month': '2018-10',
            'day': '2018-10-20',
            'hour': '2018-10-20T12',
            'minute': '2018-10-20T12:05'
        },
        'unit': 'minute',
        'parent': '2018-10-20T12',
        'sequence': 5
    }
};

test('isotrunc.to', t => {
    for (const fixture in timeFixtures) {
        for (const unit in timeFixtures[fixture].to) {
            t.equal(isotrunc.to(fixture, unit), timeFixtures[fixture].to[unit], fixture + ' to ' + unit);
        }
    }
    t.end();
});

test('isotrunc.unit', t => {
    for (const fixture in timeFixtures) {
        t.equal(isotrunc.unit(fixture), timeFixtures[fixture].unit, fixture + ' unit');
    }
    t.end();
});

test('isotrunc.parent', t => {
    for (const fixture in timeFixtures) {
        t.equal(isotrunc.parent(fixture), timeFixtures[fixture].parent, fixture + ' parent');
    }
    t.end();
});

test('isotrunc.sequence', t => {
    for (const fixture in timeFixtures) {
        t.equal(isotrunc.sequence(fixture), timeFixtures[fixture].sequence, fixture + ' sequence');
    }
    t.end();
});

test('isotrunc.parts', t => {
    for (const fixture in timeFixtures) {
        t.deepEqual(isotrunc.parts(fixture), {
            parent: isotrunc.parent(fixture),
            sequence: isotrunc.sequence(fixture)
        }, fixture + ' parts');
    }
    t.end();
});
