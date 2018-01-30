'use strict';

module.exports = {};
module.exports.to = to;
module.exports.unit = unit;
module.exports.parts = parts;
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

// minute precision is the maximum for now

const timeIndex = createIndex(timeHash);

function createIndex(hash) {
    const idx = [];
    for (const unit in hash) {
        idx[hash[unit].length] = unit;
    }
    return idx;
}

function to(ISOString, unit) {
    if (ISOString.length < timeHash[unit].length) return undefined;
    return ISOString.slice(0, timeHash[unit].length);
}

function unit(ISOString) {
    const unt = timeIndex[ISOString.length];
    const fallbackSmalledUnit = 16;
    return (unt !== undefined) ? unt : timeIndex[fallbackSmalledUnit];
}

function parts(ISOString) {
    return {
        parent: parent(ISOString),
        sequence: sequence(ISOString)
    };
}

function parent(ISOString) {
    const unt = unit(ISOString);
    const parentUnt = timeHash[unt].parent;
    if (parentUnt === undefined) return undefined;
    return ISOString.slice(0, timeHash[parentUnt].length);
}

function sequence(ISOString) {
    const unt = unit(ISOString);
    let sequence;

    if (unt === 'minute') {
        sequence = new Date(ISOString).getUTCMinutes();
    } else if (unt === 'hour') {
        sequence = new Date(ISOString + ':00').getUTCHours();
    } else if (unt === 'day') {
        sequence = new Date(ISOString).getUTCDate();
    } else if (unt === 'month') {
        sequence = new Date(ISOString).getUTCMonth() + 1;
    } else if (unt === 'year') {
        sequence = undefined;
    }

    return sequence;
}
