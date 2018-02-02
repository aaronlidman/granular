'use strict';

const http = require('http');
const zlib = require('zlib');
const AWS = require('aws-sdk');

const isotrunc = require('./isotrunc.js');

module.exports = {};
module.exports.get = get;
module.exports.changes = changes;
module.exports.getGzipStream = getGzipStream;
module.exports.countItems = getCountsItems;

function get(url) {
    return new Promise((resolve, reject) => {
        const request = http.get(url, (response) => {
            if (response.statusCode !== 200) {
                return reject(new Error('statusCode: ' + response.statusCode));
            }

            let data = '';
            response.on('data', chunk => {
                data += chunk;
            });

            response.on('end', () => {
                resolve(data);
            });
        });

        request.on('error', reject);
    });
}

function changes(stateFileUrl) {
    return new Promise((resolve, reject) => {
        getGzipStream(stateFileUrl)
            .then(resolve)
            .catch(reject);
    });
}

function getGzipStream(url) {
    const request = http.get(url);

    return new Promise((resolve, reject) => {
        request.on('response', response => {
            resolve(response.pipe(zlib.createGunzip()));
        });

        request.on('error', reject);
    });
}

function getCountsItems(parent, sequence) {
    const dynamo = new AWS.DynamoDB({
        region: process.env.AWS_REGION || 'us-west-2'
    });

    return new Promise((resolve, reject) => {
        // all jobs want these things for now, that might change
        const projExpression = ['#SEQUENCE', '#OVERALLCOUNTS'];
        const xprsnAttrNames = {
            '#PARENT': 'parent',
            '#OVERALLCOUNTS': 'overallCounts',
            '#SEQUENCE': 'sequence'
        };

        const unit = isotrunc(parent + sequence).unit();

        if (unit === 'minute' || unit === 'hour' || unit === 'day') {
            projExpression.push('#USERCOUNTS');
            xprsnAttrNames['#USERCOUNTS'] = 'userCounts';
        }

        const queryValues = {':parent': {S: parent}};
        let keyValues = '#PARENT = :parent';

        if (sequence) {
            queryValues[':sequence'] = {N: sequence.toString()};
            keyValues += ' AND #SEQUENCE = :sequence';
        }

        dynamo.query({
            TableName: process.env.MainTable,
            Select: 'SPECIFIC_ATTRIBUTES',
            KeyConditionExpression: keyValues,
            ProjectionExpression: projExpression.join(', '),
            ExpressionAttributeNames: xprsnAttrNames,
            ExpressionAttributeValues: queryValues
        }, (err, data) => {
            if (err) return reject(err);
            if (data.Items.length === 0) {
                const error = new Error('no items for ' + JSON.stringify(queryValues));
                error.name = 'NoItems';
                return reject(error);
            }
            resolve(data.Items);
        });
    });
}
