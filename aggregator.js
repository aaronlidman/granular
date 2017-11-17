'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');

exports.handler = (event) => {
    const dynamo = new AWS.DynamoDB({
        region: process.env.AWS_REGION || 'us-west-2'
    });

    // remove any duplicates
    let updates = event.Records.reduce((sum, record) => {
        return sum.add(record.dynamodb.Keys.minute.S);
    }, new Set());

    updates.forEach((key) => {
        dynamo.query({
            TableName: process.env.MinutesTable,
            Select: 'SPECIFIC_ATTRIBUTES',
            KeyConditionExpression: '#MIN = :minute and begins_with(#SEQ, :sequence)',
            ProjectionExpression: '#STATS',
            ExpressionAttributeNames: {
                '#MIN': 'minute',
                '#SEQ': 'sequence',
                '#STATS': 'stats'
            },
            ExpressionAttributeValues: {
                ':minute': {S: key},
                ':sequence': {S: '00'}
            }
        }, (err, data) => {
            if (err) return console.log('error querying', err);

            const reducedStats = data.Items.reduce((sum, item) => {
                const stats = JSON.parse(zlib.gunzipSync(Buffer.from(item.stats.B, 'base64')));
                const overall = stats['_overall'];
                return {
                    create: sum.create + (overall.create_node || 0) + (overall.create_way || 0) + (overall.create_relation || 0),
                    modify: sum.modify + (overall.modify_node || 0) + (overall.modify_way || 0) + (overall.modify_relation || 0),
                    delete: sum.delete + (overall.delete_node || 0) + (overall.delete_way || 0) + (overall.delete_relation || 0)
                };
            }, {
                create: 0,
                modify: 0,
                delete: 0
            });

            insertCounts(dynamo, key, reducedStats, false);
        });
    });
};

function insertCounts(dynamo, minuteKey, counts, retry) {
    const date = new Date(minuteKey);
    const year = date.getUTCFullYear();
    const monthAndDate = [date.getUTCMonth() + 1, date.getUTCDate()].join('-');
    const offset = (date.getUTCHours() * 60) + date.getUTCMinutes();

    dynamo.updateItem({
        TableName: process.env.DaysTable,
        Key: {
            'year': {N: year.toString()},
            'day': {S: monthAndDate}
        },
        ConditionExpression: 'attribute_exists(#CREATE) AND attribute_exists(#MODIFY) AND attribute_exists(#DELETE)',
        UpdateExpression: 'SET #CREATE[' + offset + '] = :create, #MODIFY[' + offset + '] = :modify, #DELETE[' + offset + '] = :delete',
        ExpressionAttributeNames: {
            '#CREATE': 'create',
            '#MODIFY': 'modify',
            '#DELETE': 'delete'
        },
        ExpressionAttributeValues: {
            ':create': {N: counts.create.toString()},
            ':modify': {N: counts.modify.toString()},
            ':delete': {N: counts.delete.toString()}
        }
    }, (err, data) => {
        if (err) {
            if (!retry && err.code === 'ConditionalCheckFailedException') {
                dynamo.updateItem({
                    TableName: process.env.DaysTable,
                    Key: {
                        'year': {N: year.toString()},
                        'day': {S: monthAndDate}
                    },
                    UpdateExpression: 'SET #CREATE = if_not_exists(#CREATE, :list), #MODIFY = if_not_exists(#MODIFY, :list), #DELETE = if_not_exists(#DELETE, :list)',
                    ExpressionAttributeNames: {
                        '#CREATE': 'create',
                        '#MODIFY': 'modify',
                        '#DELETE': 'delete'
                    },
                    ExpressionAttributeValues: {
                        ':list': {L: new Array(1440).fill(-1).map((item) => {
                            return {'N': item.toString()};
                        })}
                    }
                }, (err) => {
                    if (err) return console.log(err);

                    console.log('prepped ' + minuteKey + ', now going to insert');
                    return setTimeout(() => {
                        insertCounts(dynamo, minuteKey, counts, true);
                    }, 2500);
                });
            } else {
                return console.log('insert error', err);
            }
        } else {
            console.log('inserted', minuteKey, data);
        }
    });
}
