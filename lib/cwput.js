'use strict';

const AWS = require('aws-sdk');
const cw = new AWS.CloudWatch({
    region: process.env.AWS_REGION || 'us-west-2'
});

module.exports = {
    overallMetrics: function (stats, time) {
        const overall = stats['_overall'];
        const data = [
            {MetricName: 'nodes_created', Value: overall.cnode || 0},
            {MetricName: 'nodes_modified', Value: overall.mnode || 0},
            {MetricName: 'nodes_deleted', Value: overall.dnode || 0},
            {MetricName: 'ways_created', Value: overall.cway || 0},
            {MetricName: 'ways_modified', Value: overall.mway || 0},
            {MetricName: 'ways_deleted', Value: overall.dway || 0},
            {MetricName: 'relations_created', Value: overall.crelation || 0},
            {MetricName: 'relations_modified', Value: overall.mrelation || 0},
            {MetricName: 'relations_deleted', Value: overall.drelation || 0}
        ];

        return new Promise((resolve, reject) => {
            cw.putMetricData({
                MetricData: data,
                Namespace: ('osm-dash-' + process.env.Environment)
            }, (err) => {
                if (err) return reject(err);
                return resolve(stats);
            });
        });
    }
};
