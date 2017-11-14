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
            TableName: process.env.MinutesTableName,
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
            if (err) return console.log('querying', err);

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

            console.log(key, reducedStats);
        });
    });
};
