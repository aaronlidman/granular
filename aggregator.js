'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');
const dynamo = new AWS.DynamoDB({
    region: process.env.AWS_REGION || 'us-west-2'
});

exports.getCounts = getCounts;
exports.mergeCounts = mergeCounts;
exports.allocateLists = allocateLists;
exports.writeCounts = writeCounts;

exports.handler = event => {
    removeDuplicates(event.Records)
        .then(getCounts)
        .then(mergeCounts)
        .then(writeCounts)
        .catch(console.log);
};

function removeDuplicates(records) {
    let uniqueMinutes = records.reduce((sum, record) => {
        return sum.add(record.dynamodb.Keys.minute.S);
    }, new Set());

    return Promise.resolve(uniqueMinutes);
}

function getCounts(minutes) {
    let promises = [];

    minutes.forEach((key) => {
        let query = new Promise((resolve, reject) => {
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
                if (err) return reject(err);
                resolve({key: key, counts: data.Items});
            });
        });

        promises.push(query);
    });

    return Promise.all(promises);
}

function mergeCounts(counts) {
    // reduce down multiple stats for the same time period into one
    // 5:15 - created 5
    // 5:15 - created 2
    // 5:15 - created 1
    // => 5:15 - created 8

    counts = counts.map(countObj => {
        const reducedStats = countObj.counts.reduce((sum, item) => {
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

        return {key: countObj.key, counts: reducedStats};
    });

    return counts;
}

function allocateLists(keyObject) {
    return new Promise((resolve, reject) => {
        dynamo.updateItem({
            TableName: process.env.DaysTable,
            Key: {
                'year': {N: keyObject.year.toString()},
                'day': {S: keyObject.monthAndDate}
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
            if (err) return reject(err);
            resolve();
        });
    });
}

function writeCounts(counts, retry) {
    let promises = [];

    counts.forEach((countObj) => {
        let update = new Promise((resolve, reject) => {
            const date = new Date(countObj.key);
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
                    ':create': {N: countObj.counts.create.toString()},
                    ':modify': {N: countObj.counts.modify.toString()},
                    ':delete': {N: countObj.counts.delete.toString()}
                }
            }, (err, data) => {
                if (!err) return resolve(data);

                if (err.code === 'ConditionalCheckFailedException' && !retry) {
                    allocateLists({year: year, monthAndDate: monthAndDate})
                        .then(() => { writeCounts([countObj], true); })
                        .then(resolve)
                        .catch(reject);
                } else {
                    return reject(err);
                }
            });
        });

        promises.push(update);
    });

    return Promise.all(promises);
}
