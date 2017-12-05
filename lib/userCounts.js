'use strict';

module.exports = {};
module.exports.toCSV = toCSV;
module.exports.merge = mergeUserCounts;
module.exports.pack = packUserCounts;

const zlib = require('zlib');

function toCSV(obj) {
    for (const period in obj.stats) {
        let userString = '';
        for (const user in obj.stats[period].userCounts) {
            const current = obj.stats[period].userCounts[user];

            // merging these for now, will break them out later if it won't clutter the UI
            const created = current.create_node + current.create_way + current.create_relation;
            const modified = current.modify_node + current.modify_way + current.modify_relation;
            const deleted = current.delete_node + current.delete_way + current.delete_relation;

            userString += [user.split(',').join('.'), created, modified, deleted].join(',') + '\n';
        }

        obj.stats[period].userCounts = userString;
    }

    return obj;
}

function mergeUserCounts(context) {
    const userCounts = context.job.children.reduce((sum, count) => {
        sum += zlib.gunzipSync(count.userCounts.B).toString();
        return sum;
    }, '');

    // depending on the job type we might have to group stuff here.

    if (!context.job.data) context.job.data = {};
    context.job.data.userCounts = userCounts;
    return context;
}

function packUserCounts(context) {
    // pack values for dynamo
    if (context.job.data.userCounts) {
        context.job.data.userCounts = {
            B: zlib.gzipSync(context.job.data.userCounts)
        };
    }
    return context;
}
