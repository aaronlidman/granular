'use strict';

module.exports = {};
module.exports.toCSV = toCSV;

function toCSV(obj) {
    for (const period in obj.stats) {
        let overallString = '';

        const current = obj.stats[period].overallCounts;
        const created = current.create_node + current.create_way + current.create_relation;
        const modified = current.modify_node + current.modify_way + current.modify_relation;
        const deleted = current.delete_node + current.delete_way + current.delete_relation;

        overallString += [created, modified, deleted].join(',') + '\n';

        obj.stats[period].overallCounts = overallString;
    }

    return obj;
}
