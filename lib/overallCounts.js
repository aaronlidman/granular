'use strict';

const zlib = require('zlib');

module.exports = {};
module.exports.mergeTotals = mergeTotals;
module.exports.collapseTotals = collapseTotals;
module.exports.arrayTotals = arrayTotals;
module.exports.mergeArrays = mergeArrays;

function mergeTotals(overallCounts) {
    // reduces an array of individual counts into a single count, per attribute
    // used to collapse diffuse counts for the same time period into one
    // just like userCounts, but simpler, focued only on actions

    overallCounts = overallCounts.reduce((sum, overallCount) => {
        const counts = JSON.parse(zlib.gunzipSync(overallCount.overallCounts.B));

        for (const action in counts) {
            if (!sum[action]) sum[action] = 0;
            sum[action] += counts[action];
        }

        return sum;
    }, {});

    return overallCounts;
}

function collapseTotals(overallCount) {
    // merges counts for all geometry types together
    // so, create_node + create_way + create_relation = `create` and so on

    const newCounts = {
        'create': 0,
        'modify': 0,
        'delete': 0
    };

    for (const action in overallCount) {
        let newAction = null;

        if (action.indexOf('create') > -1) newAction = 'create';
        if (action.indexOf('modify') > -1) newAction = 'modify';
        if (action.indexOf('delete') > -1) newAction = 'delete';

        newCounts[newAction] += overallCount[action];
    }

    return newCounts;
}

function arrayTotals(overallCounts) {
    // array (v) all the minutely totals into an array for the hour

    const countsArrays = {};

    overallCounts.forEach(child => {
        const totalsObj = JSON.parse(zlib.gunzipSync(child.overallCounts.B));
        const sequence = parseInt(child.sequence.N);

        for (const action in totalsObj) {
            // totalsObj[action] is an integer
            if (!countsArrays[action]) countsArrays[action] = new Array(60).fill(-1);
            countsArrays[action][sequence] = totalsObj[action];
        }
    });

    return countsArrays;
}

function mergeArrays(period, children) {
    // not the most proud of this but it's the general idea

    let expectedChildren = 0;
    let expectedChildLength = 0;

    if (period === 'day') {
        expectedChildren = 24;
        expectedChildLength = 60;
    }

    const merged = {};

    children.forEach(child => {
        const totalsObj = JSON.parse(zlib.gunzipSync(child.overallCounts.B));
        const sequence = parseInt(child.sequence.N);

        for (const action in totalsObj) {
            // totalsObj[action] is an array
            if (!merged[action]) merged[action] = new Array(expectedChildren).fill(new Array(expectedChildLength).fill(-1));
            merged[action][sequence] = totalsObj[action];
        }
    });

    for (const action in merged) {
        merged[action] = flatten(merged[action]);
    }

    return merged;
}

function flatten(arrayOfArrays) {
    return arrayOfArrays.reduce((sum, array) => {
        return sum.concat(array);
    }, []);
}
