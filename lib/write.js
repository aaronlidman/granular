'use strict';

const zlib = require('zlib');
const AWS = require('aws-sdk');

const isotrunc = require('./isotrunc.js');

module.exports = {};
module.exports.fetcherStats = writeFetcherStats;
module.exports.aggregate = writeAggregate;

function writeFetcherStats(obj) {
    const dynamo = new AWS.DynamoDB({region: process.env.AWS_REGION || 'us-west-2'});

    let promises = [];

    for (const minute in obj.stats) {
        const put = new Promise((resolve, reject) => {
            if (!obj.state || !obj.state.sequenceNumber) {
                return reject(new Error('missing sequenceNumber'));
            }

            const key = isotrunc.to(minute, 'minute');
            dynamo.updateItem({
                TableName: process.env.MainTable,
                Key: {
                    parent: {S: key},
                    sequence: {N: obj.state.sequenceNumber}
                },
                UpdateExpression: 'SET #USERCOUNTS = :userCounts, #OVERALLCOUNTS = :overallCounts',
                ExpressionAttributeNames: {
                    '#USERCOUNTS': 'userCounts',
                    '#OVERALLCOUNTS': 'overallCounts'
                },
                ExpressionAttributeValues: {
                    ':userCounts': {B: zlib.gzipSync(obj.stats[minute].userCounts)},
                    ':overallCounts': {B: zlib.gzipSync(obj.stats[minute].overallCounts)}
                }
            }, (err, data) => {
                if (err) return reject(err);
                resolve(key);
            });
        });

        promises.push(put);
    }

    return Promise.all(promises);
}


function writeAggregate(compositeKey, data) {
    // dynamically builds the right statements depending on what's in data
    // will be more useful later with bigger aggregates that can't have all data

    // all values must be binary
    const dynamo = new AWS.DynamoDB({region: process.env.AWS_REGION || 'us-west-2'});

    return new Promise((resolve, reject) => {
        const attrs = [];
        for (const attr in data) {
            attrs.push(attr);
        }

        const attrNames = {};
        const updateExpression = [];
        const attrValues = {};

        attrs.forEach(attr => {
            const subName = '#' + attr.toUpperCase();
            const subValue = ':' + attr;
            attrNames[subName] = attr;
            updateExpression.push(subName + ' = ' + subValue);
            attrValues[subValue] = {B: data[attr]};
        });

        dynamo.updateItem({
            TableName: process.env.MainTable,
            Key: {
                parent: {S: compositeKey.parent},
                sequence: {N: compositeKey.sequence}
            },
            UpdateExpression: 'SET ' + updateExpression.join(', '),
            ExpressionAttributeNames: attrNames,
            ExpressionAttributeValues: attrValues
        }, (err, data) => {
            if (err) return reject(err);
            resolve(data);
        });
    });
}
