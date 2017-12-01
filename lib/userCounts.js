'use strict';

module.exports = {};
module.exports.toCSV = toCSV;

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
