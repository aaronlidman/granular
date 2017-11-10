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
            const timestamp = new Date(minute).toISOString();
            const data = [
                {
                    MetricName: 'nodes_created',
                    Value: overall.create_node || 0,
                    Timestamp: timestamp
                }, {
                    MetricName: 'nodes_modified',
                    Value: overall.modify_node || 0,
                    Timestamp: timestamp
                }, {
                    MetricName: 'nodes_deleted',
                    Value: overall.delete_node || 0,
                    Timestamp: timestamp
                }, {
                    MetricName: 'ways_created',
                    Value: overall.create_way || 0,
                    Timestamp: timestamp
                }, {
                    MetricName: 'ways_modified',
                    Value: overall.modify_way || 0,
                    Timestamp: timestamp
                }, {
                    MetricName: 'ways_deleted',
                    Value: overall.delete_way || 0,
                    Timestamp: timestamp
                }, {
                    MetricName: 'relations_created',
                    Value: overall.create_relation || 0,
                    Timestamp: timestamp
                }, {
                    MetricName: 'relations_modified',
                    Value: overall.modify_relation || 0,
                    Timestamp: timestamp
                }, {
                    MetricName: 'relations_deleted',
                    Value: overall.delete_relation || 0,
                    Timestamp: timestamp
                }, {
                    MetricName: 'active_users',
                    Value: Object.keys(obj.stats[minute]).length - 1,
                    Timestamp: timestamp
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
