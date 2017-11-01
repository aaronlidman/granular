'use strict';

const AWS = require('aws-sdk');
const cw = new AWS.CloudWatch({
    region: process.env.AWS_REGION || 'us-west-2'
});

module.exports = {
    overallMetrics: function (obj) {
        let promises = [];

        for (const minute in obj.stats) {
            const overall = obj.stats[minute]['_overall'];
            const data = [
                {
                    MetricName: 'nodes_created',
                    Value: overall.c_node || 0,
                    Timestamp: minute
                }, {
                    MetricName: 'nodes_modified',
                    Value: overall.m_node || 0,
                    Timestamp: minute
                }, {
                    MetricName: 'nodes_deleted',
                    Value: overall.d_node || 0,
                    Timestamp: minute
                }, {
                    MetricName: 'ways_created',
                    Value: overall.c_way || 0,
                    Timestamp: minute
                }, {
                    MetricName: 'ways_modified',
                    Value: overall.m_way || 0,
                    Timestamp: minute
                }, {
                    MetricName: 'ways_deleted',
                    Value: overall.d_way || 0,
                    Timestamp: minute
                }, {
                    MetricName: 'relations_created',
                    Value: overall.c_relation || 0,
                    Timestamp: minute
                }, {
                    MetricName: 'relations_modified',
                    Value: overall.m_relation || 0,
                    Timestamp: minute
                }, {
                    MetricName: 'relations_deleted',
                    Value: overall.d_relation || 0,
                    Timestamp: minute
                }, {
                    MetricName: 'active_users',
                    Value: Object.keys(obj.stats[minute]).length - 1,
                    Timestamp: minute
                }
            ];

            const put = new Promise((resolve, reject) => {
                cw.putMetricData({
                    MetricData: data,
                    Namespace: ('osm-dash-' + process.env.Environment)
                }, (err) => {
                    if (err) return reject(err);
                    resolve();
                });
            });

            promises.push(put);
        }

        return new Promise((resolve, reject) => {
            Promise
                .all(promises)
                .then(() => {
                    resolve({
                        state: obj.state,
                        stats: obj.stats
                    });
                })
                .catch(reject);
        });
    }
};
