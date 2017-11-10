'use strict';

const zlib = require('zlib');
const AWS = require('aws-sdk');

module.exports = {
    minutelyStats: function (obj) {
        const dynamo = new AWS.DynamoDB({
            region: process.env.AWS_REGION || 'us-west-2'
        });
        const cw = new AWS.CloudWatch({
            region: process.env.AWS_REGION || 'us-west-2'
        });
        // aws services required in here for test mocking

        let promises = [];

        for (const minute in obj.stats) {
            const put = new Promise((resolve, reject) => {
                if (!obj.state || !obj.state.sequenceNumber) return reject('missing sequenceNumber');

                const key = minute.slice(0, 16) + '-' + obj.state.sequenceNumber;
                const value = zlib.gzipSync(JSON.stringify(obj.stats[minute]));

                dynamo.updateItem({
                    TableName: ['osm-dash', process.env.Environment, 'minutes'].join('-'),
                    Key: {
                        'minute': {
                            S: key
                        }
                    },
                    UpdateExpression: 'SET #STATS = :stats',
                    ExpressionAttributeNames: {
                        '#STATS': 'stats'
                    },
                    ExpressionAttributeValues: {
                        ':stats': {B: value}
                    },
                }, (err, data) => {
                    if (err) return reject(err);
                    resolve(data);
                });
            });

            promises.push(put);
        }

        return new Promise((resolve, reject) => {
            Promise.all(promises)
                .then((data) => {
                    const metrics = [{
                        MetricName: 'files_written',
                        Value: data.length,
                        Timestamp: new Date().toISOString()
                    }];

                    cw.putMetricData({
                        MetricData: metrics,
                        Namespace: ('osm-dash-' + process.env.Environment)
                    }, (err) => {
                        if (err) return reject(err);
                        resolve(data);
                    });
                })
                .catch(reject);
        });
    }
};
