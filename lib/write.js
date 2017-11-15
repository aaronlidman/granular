'use strict';

const zlib = require('zlib');
const AWS = require('aws-sdk');

module.exports = {
    minutelyStats: function (obj) {
        const dynamo = new AWS.DynamoDB({
            region: process.env.AWS_REGION || 'us-west-2'
        });
        // services required in here for test mocking

        let promises = [];

        for (const minute in obj.stats) {
            const put = new Promise((resolve, reject) => {
                if (!obj.state || !obj.state.sequenceNumber) return reject('missing sequenceNumber');

                dynamo.updateItem({
                    TableName: process.env.MinutesTable,
                    Key: {
                        'minute': {
                            S: minute.slice(0, 16)
                        },
                        'sequence': {
                            S: obj.state.sequenceNumber
                        }
                    },
                    UpdateExpression: 'SET #STATS = :stats',
                    ExpressionAttributeNames: {
                        '#STATS': 'stats'
                    },
                    ExpressionAttributeValues: {
                        ':stats': {
                            B: zlib.gzipSync(JSON.stringify(obj.stats[minute]))
                        }
                    },
                }, (err, data) => {
                    if (err) return reject(err);
                    resolve(data);
                });
            });

            promises.push(put);
        }

        return Promise.all(promises);
    }
};
