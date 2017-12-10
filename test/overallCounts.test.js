'use strict';

const test = require('tape');
const overallCounts = require('../lib/overallCounts.js');

test('overallCounts', t => {
    const result = overallCounts.toCSV({
        stats: {
            '2017-01-01T00:00:47': {
                overallCounts: {
                    create_node: 12,
                    modify_node: 2,
                    delete_node: 8,
                    create_way: 77,
                    modify_way: 0,
                    delete_way: 9,
                    create_relation: 1,
                    modify_relation: 2,
                    delete_relation: 3
                }
            },
            'time2': {
                overallCounts: {
                    create_node: 2,
                    modify_node: 2,
                    delete_node: 2,
                    create_way: 2,
                    modify_way: 2,
                    delete_way: 2,
                    create_relation: 2,
                    modify_relation: 2,
                    delete_relation: 2
                }
            },
            'anytimeperiod': {
                overallCounts: {
                    create_node: 1,
                    modify_node: 0,
                    delete_node: 0,
                    create_way: 0,
                    modify_way: 0,
                    delete_way: 0,
                    create_relation: 0,
                    modify_relation: 0,
                    delete_relation: 0
                }
            }
        }
    });

    t.deepEqual(result, {stats: {
        '2017-01-01T00:00:47': {overallCounts: '2017-01-01T00:00:47,90,4,20\n'},
        'time2': {overallCounts: 'time2,6,6,6\n'},
        'anytimeperiod': {overallCounts: 'anytimeperiod,1,0,0\n'}}
    });

    t.end();
});
