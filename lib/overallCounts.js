'use strict';

module.exports = {};
module.exports.toCSV = toCSV;
module.exports.merge = mergeOverallCounts;
module.exports.pack = packOverallCounts;

const zlib = require('zlib');

function toCSV(obj) {
    for (const period in obj.stats) {
        let overallString = '';

        const current = obj.stats[period].overallCounts;
        const created = current.create_node + current.create_way + current.create_relation;
        const modified = current.modify_node + current.modify_way + current.modify_relation;
        const deleted = current.delete_node + current.delete_way + current.delete_relation;

        overallString += [period, created, modified, deleted].join(',') + '\n';

        obj.stats[period].overallCounts = overallString;
    }

    return obj;
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

function packOverallCounts(context) {
    // pack values for dynamo
    if (context.message.data.overallCounts) {
        context.message.data.overallCounts = {
            B: zlib.gzipSync(context.message.data.overallCounts)
        };
    }
    return context;
}
