'use strict';

let context = {
    original: false
};

module.exports = function (ISOString) {
    if (!ISOString) throw Error('Must specify an ISOString to truncate');

    context.original = ISOString;

    return {
        to: to,
        unit: unit,
        parts: parts,
        parent: parent,
        sequence: sequence
    };
};

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
// year precision is the minimum for now

const timeIndex = createIndex(timeHash);

function createIndex(hash) {
    const idx = [];
    for (const unit in hash) {
        idx[hash[unit].length] = unit;
    }
    return idx;
}

function to(unit) {
    if (context.original.length < timeHash[unit].length) return undefined;
    return context.original.slice(0, timeHash[unit].length);
}

function unit() {
    const unt = timeIndex[context.original.length];
    const fallbackSmalledUnit = 16;
    return (unt !== undefined) ? unt : timeIndex[fallbackSmalledUnit];
}

function parts() {
    return {
        parent: parent(),
        sequence: sequence()
    };
}

function parent() {
    const unt = unit();
    const parentUnt = timeHash[unt].parent;
    return context.original.slice(0, timeHash[parentUnt].length);
}

function sequence() {
    const unt = unit(context.original);
    let sequence;

    if (unt === 'minute') {
        sequence = new Date(context.original).getUTCMinutes();
    } else if (unt === 'hour') {
        sequence = new Date(context.original + ':00').getUTCHours();
    } else if (unt === 'day') {
        sequence = new Date(context.original).getUTCDate();
    } else if (unt === 'month') {
        sequence = new Date(context.original).getUTCMonth() + 1;
    } else if (unt === 'year') {
        sequence = undefined;
    }

    return sequence;
}
