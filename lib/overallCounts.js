'use strict';

module.exports = {};
module.exports.toCSV = toCSV;
module.exports.merge = mergeOverallCounts;

const zlib = require('zlib');

function toCSV(stats) {
    for (const period in stats) {
        let overallString = '';

        const current = stats[period].overallCounts;
        const created = current.create_node + current.create_way + current.create_relation;
        const modified = current.modify_node + current.modify_way + current.modify_relation;
        const deleted = current.delete_node + current.delete_way + current.delete_relation;

        overallString += [period, created, modified, deleted].join(',') + '\n';

        stats[period].overallCounts = overallString;
    }

    return stats;
}

function mergeOverallCounts(context) {
    const overallCounts = context.message.children.reduce((sum, count) => {
        sum += zlib.gunzipSync(count.overallCounts.B).toString();
        return sum;
    }, '');

    // depending on the job type we might have to group stuff here.

    if (!context.message.data) context.message.data = {};
    context.message.data.overallCounts = overallCounts;
    return context;
}
