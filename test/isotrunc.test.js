'use strict';

const test = require('tape');
const isotrunc = require('../lib/isotrunc.js');

const fixtures = {
    '2018': {
        'to': {
            'year': '2018'
        },
        'unit': 'year',
        'parent': '2018',
        'sequence': undefined
    },
    '2018-10': {
        'to': {
            'year': '2018',
            'month': '2018-10'
        },
        'unit': 'month',
        'parent': '2018',
        'sequence': 10
    },
    '2018-10-20': {
        'to': {
            'year': '2018',
            'month': '2018-10',
            'day': '2018-10-20'
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
            'hour': '2018-10-20T12'
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

test('throw error on no input', t => {
    try {
        isotrunc();
    } catch (e) {
        t.equal(e.message, 'Must specify an ISOString to truncate');
    }

    t.end();
});

test('isotrunc.to', t => {
    for (const fixture in fixtures) {
        for (const unit in fixtures[fixture].to) {
            t.equal(isotrunc(fixture).to(unit), fixtures[fixture].to[unit], fixture + ' to ' + unit);
        }
    }
    t.end();
});

test('isotrunc.unit', t => {
    for (const fixture in fixtures) {
        t.equal(isotrunc(fixture).unit(), fixtures[fixture].unit, fixture + ' unit');
    }
    t.end();
});

test('isotrunc.parent', t => {
    for (const fixture in fixtures) {
        t.equal(isotrunc(fixture).parent(), fixtures[fixture].parent, fixture + ' parent');
    }
    t.end();
});

test('isotrunc.sequence', t => {
    for (const fixture in fixtures) {
        t.equal(isotrunc(fixture).sequence(), fixtures[fixture].sequence, fixture + ' sequence');
    }
    t.end();
});

test('isotrunc.parts', t => {
    for (const fixture in fixtures) {
        t.deepEqual(isotrunc(fixture).parts(), {
            parent: isotrunc(fixture).parent(),
            sequence: isotrunc(fixture).sequence()
        }, fixture + ' parts');
    }
    t.end();
});
