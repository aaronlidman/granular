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
    return timeIndex[ISOString.length];
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
        sequence = new Date(ISOString).getUTCFullYear();
    }

    return sequence;
}
