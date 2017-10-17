'use strict';

const AWS = require('aws-sdk');
const cw = new AWS.CloudWatch({
    region: process.env.AWS_REGION || 'us-west-2'
});

module.exports = {
    overallMetrics: function (allUserStats) {
        const overallStats = allUserStats['_overall'];
        const data = [
            {MetricName: 'nodes_created', Value: overallStats.cnode || 0},
            {MetricName: 'nodes_modified', Value: overallStats.mnode || 0},
            {MetricName: 'nodes_deleted', Value: overallStats.dnode || 0},
            {MetricName: 'ways_created', Value: overallStats.cway || 0},
            {MetricName: 'ways_modified', Value: overallStats.mway || 0},
            {MetricName: 'ways_deleted', Value: overallStats.dway || 0},
            {MetricName: 'relations_created', Value: overallStats.crelation || 0},
            {MetricName: 'relations_modified', Value: overallStats.mrelation || 0},
            {MetricName: 'relations_deleted', Value: overallStats.drelation || 0}
        ];

        return new Promise((resolve, reject) => {
            cw.putMetricData({
                MetricData: data,
                Namespace: ('osm-dash-' + process.env.Environment)
            }, (err) => {
                if (err) return reject(err);
                return resolve(allUserStats);
            });
        });
    }
};
