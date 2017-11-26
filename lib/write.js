'use strict';

const zlib = require('zlib');
const AWS = require('aws-sdk');

module.exports = {
    minutelyStats: function (statsObj) {
        const dynamo = new AWS.DynamoDB({
            region: process.env.AWS_REGION || 'us-west-2'
        });

        let promises = [];

        for (const minute in statsObj.stats) {
            const put = new Promise((resolve, reject) => {
                if (!statsObj.state || !statsObj.state.sequenceNumber) return reject('missing sequenceNumber');

                dynamo.updateItem({
                    TableName: process.env.SequenceTable,
                    Key: {
                        'minute': {
                            S: minute.slice(0, 16)
                        },
                        'sequence': {
                            S: statsObj.state.sequenceNumber
                        }
                    },
                    UpdateExpression: 'SET #USERCOUNTS = :userCounts, #OVERALLCOUNTS = :overallCounts',
                    ExpressionAttributeNames: {
                        '#USERCOUNTS': 'userCounts',
                        '#OVERALLCOUNTS': 'overallCounts'
                    },
                    ExpressionAttributeValues: {
                        ':userCounts': {
                            B: zlib.gzipSync(JSON.stringify(statsObj.stats[minute].userCounts))
                        },
                        ':overallCounts': {
                            B: zlib.gzipSync(JSON.stringify(statsObj.stats[minute].overallCounts))
                        }
                    }
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
