'use strict';

const AWS = require('aws-sdk');
const cw = new AWS.CloudWatch({
    region: process.env.AWS_REGION || 'us-west-2'
});

module.exports = {
    overallMetrics: function (overallStats) {
        const data = [
            {
                MetricName: 'nodes_created',
                Value: overallStats.cnode
            },
            {
                MetricName: 'nodes_modified',
                Value: overallStats.mnode
            },
            {
                MetricName: 'nodes_deleted',
                Value: overallStats.dnode
            },
            {
                MetricName: 'ways_created',
                Value: overallStats.cway
            },
            {
                MetricName: 'ways_modified',
                Value: overallStats.mway
            },
            {
                MetricName: 'ways_deleted',
                Value: overallStats.dway
            },
            {
                MetricName: 'relations_created',
                Value: overallStats.crelation
            },
            {
                MetricName: 'relations_modified',
                Value: overallStats.mrelation
            },
            {
                MetricName: 'relations_deleted',
                Value: overallStats.drelation
            },
        ];

        cw.putMetricData({
            MetricData: data,
            Namespace: 'osm-dash'
        }, (err, data) => {
            if (err) return new Promise.reject(err);
            return new Promise.resolve(data);
        });

    }
};
