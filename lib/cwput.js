'use strict';

const AWS = require('aws-sdk');
const cw = new AWS.CloudWatch({
    region: process.env.AWS_REGION || 'us-west-2'
});

module.exports = {
    overallMetrics: function (obj) {
        let time = obj.time;

        const overall = obj.stats['_overall'];
        const data = [
            {
                MetricName: 'nodes_created',
                Value: overall.cnode || 0,
                Timestamp: time
            }, {
                MetricName: 'nodes_modified',
                Value: overall.mnode || 0,
                Timestamp: time
            }, {
                MetricName: 'nodes_deleted',
                Value: overall.dnode || 0,
                Timestamp: time
            }, {
                MetricName: 'ways_created',
                Value: overall.cway || 0,
                Timestamp: time
            }, {
                MetricName: 'ways_modified',
                Value: overall.mway || 0,
                Timestamp: time
            }, {
                MetricName: 'ways_deleted',
                Value: overall.dway || 0,
                Timestamp: time
            }, {
                MetricName: 'relations_created',
                Value: overall.crelation || 0,
                Timestamp: time
            }, {
                MetricName: 'relations_modified',
                Value: overall.mrelation || 0,
                Timestamp: time
            }, {
                MetricName: 'relations_deleted',
                Value: overall.drelation || 0,
                Timestamp: time
            }, {
                MetricName: 'active_users',
                Value: Object.keys(obj.stats).length - 1,
                Timestamp: time
            }
        ];

        return new Promise((resolve, reject) => {
            cw.putMetricData({
                MetricData: data,
                Namespace: ('osm-dash-' + process.env.Environment)
            }, (err) => {
                if (err) return reject(err);

                resolve({
                    state: obj.state,
                    stats: obj.stats,
                    time: obj.time
                });
            });
        });
    }
};
