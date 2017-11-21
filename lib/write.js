'use strict';

const AWS = require('aws-sdk');
const dynamo = new AWS.DynamoDB({
    region: process.env.AWS_REGION || 'us-west-2'
});

module.exports = {
    minutelyStats: function (statsObj) {
        let promises = [];

        for (const minute in statsObj.stats) {
            const put = new Promise((resolve, reject) => {
                if (!statsObj.state || !statsObj.state.sequenceNumber) return reject('missing sequenceNumber');

                const overall = statsObj.stats[minute]['_overall'];
                const compositeKey = {
                    'parent': {
                        S: minute.slice(0, 16)
                    },
                    'sequence': {
                        N: statsObj.state.sequenceNumber
                    }
                };

                dynamo.updateItem({
                    TableName: process.env.TimeTable,
                    Key: compositeKey,
                    UpdateExpression: 'SET #CREATE = :create, #DELETE = :delete, #MODIFY = :modify, #USERS = :users',
                    ExpressionAttributeNames: {
                        '#CREATE': 'create',
                        '#MODIFY': 'modify',
                        '#DELETE': 'delete',
                        '#USERS': 'users'
                    },
                    ExpressionAttributeValues: {
                        ':create': {
                            N: ((overall.create_node || 0) + (overall.create_way || 0) + (overall.create_relation || 0)).toString()
                        },
                        ':modify': {
                            N: ((overall.modify_node || 0) + (overall.modify_way || 0) + (overall.modify_relation || 0)).toString()
                        },
                        ':delete': {
                            N: ((overall.delete_node || 0) + (overall.delete_way || 0) + (overall.delete_relation || 0)).toString()
                        },
                        ':users': {
                            SS: Object.keys(statsObj.stats[minute])
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
