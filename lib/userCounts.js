'use strict';

const zlib = require('zlib');

module.exports = {};
module.exports.mergeTotals = mergeTotals;
module.exports.collapseTotals = collapseTotals;
module.exports.arrayMinuteTotals = arrayMinuteTotals;

function mergeTotals(userCounts) {
    // reduces an array of individual counts into a single count per user, per attribute
    // used to collapse diffuse counts for the same time period into one

    userCounts = userCounts.reduce((sum, userCount) => {
        const counts = JSON.parse(zlib.gunzipSync(userCount.userCounts.B));

        for (const user in counts) {
            if (!sum[user]) sum[user] = {};

            for (const metric in counts[user]) {
                if (!sum[user][metric]) sum[user][metric] = 0;
                sum[user][metric] += counts[user][metric];
            }
        }

        return sum;
    }, {});

    return userCounts;
}

function collapseTotals(userCount) {
    // merges counts for all geometry types together
    // so, create_node + create_way + create_relation = `create`
    const newCounts = {};

    for (const user in userCount) {
        const count = userCount[user];
        newCounts[user] = {
            created: (count.create_node || 0) + (count.create_way || 0) + (count.create_relation || 0),
            modified: (count.modify_node || 0) + (count.modify_way || 0) + (count.modify_relation || 0),
            deleted: (count.delete_node || 0) + (count.delete_way || 0) + (count.delete_relation || 0)
        };
    }

    return newCounts;
}

function arrayMinuteTotals(userCounts) {
    // array (v) all the minutely totals into an array for the hour

    const userArrays = {};

    userCounts.forEach(child => {
        const countsObj = JSON.parse(zlib.gunzipSync(child.userCounts.B));
        const sequence = parseInt(child.sequence.N);

        for (const user in countsObj) {
            if (!userArrays[user]) userArrays[user] = {};

            for (const action in countsObj[user]) {
                if (!userArrays[user][action]) userArrays[user][action] = new Array(60);

                userArrays[user][action][sequence] = countsObj[user][action];
            }
        }
    });

    // these arrays is super sparse but they are simple and compress just as well as prettier structures
    return userArrays;
}
