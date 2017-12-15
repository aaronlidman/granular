'use strict';

module.exports = {};
module.exports.toCSV = toCSV;
module.exports.merge = mergeUserCounts;

const zlib = require('zlib');

function toCSV(stats) {
    for (const period in stats) {
        let userString = '';
        for (const user in stats[period].userCounts) {
            const current = stats[period].userCounts[user];

            // merging these for now, will break them out later if it won't clutter the UI
            const created = current.create_node + current.create_way + current.create_relation;
            const modified = current.modify_node + current.modify_way + current.modify_relation;
            const deleted = current.delete_node + current.delete_way + current.delete_relation;

            userString += [period, user.split(',').join('.'), created, modified, deleted].join(',') + '\n';
        }

        stats[period].userCounts = userString;
    }

    return stats;
}

function mergeUserCounts(context) {
    const userCounts = context.message.children.reduce((sum, count) => {
        sum += zlib.gunzipSync(count.userCounts.B).toString();
        return sum;
    }, '');

    // depending on the job type we might have to group stuff here.

    if (!context.message.data) context.message.data = {};
    context.message.data.userCounts = userCounts;
    return context;
}
