'use strict';

const zlib = require('zlib');

module.exports = {};
module.exports.mergeTotals = mergeTotals;
module.exports.collapseTotals = collapseTotals;

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

    const newCounts = {};

    for (const action in overallCount) {
        let newAction = null;

        if (action.indexOf('create') > -1) newAction = 'create';
        if (action.indexOf('modify') > -1) newAction = 'modify';
        if (action.indexOf('delete') > -1) newAction = 'delete';

        if (!newCounts[newAction]) newCounts[newAction] = 0;
        newCounts[newAction] += overallCount[action];
    }

    return newCounts;
}
