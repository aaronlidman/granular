'use strict';

module.exports = {};
module.exports.to = to;
module.exports.unit = unit;
module.exports.parent = parent;
module.exports.sequence = sequence;

const timeHash = {
    year: {
        parent: 'year',
        length: 4
    },
    month: {
        parent: 'year',
        length: 7
    },
    day: {
        parent: 'month',
        length: 10
    },
    hour: {
        parent: 'day',
        length: 13
    },
    minute: {
        parent: 'hour',
        length: 16
    }
};

const timeIndex = createIndex(timeHash);

function createIndex(hash) {
    const idx = [];
    for (const unit in hash) {
        idx[hash[unit].length] = unit;
    }
    return idx;
}

function to(ISOString, unit) {
    return ISOString.slice(0, timeHash[unit].length);
}

function unit(ISOString) {
    const unit = timeIndex[ISOString.length];
    if (unit) return unit;
}

function parent(ISOString) {
    const unt = unit(ISOString);
    const parentUnt = timeHash[unt].parent;
    return ISOString.slice(0, timeHash[parentUnt].length);
}

function sequence(ISOString) {
    // get the unit, then date parse and get the unit
    return ISOString.slice();
}
