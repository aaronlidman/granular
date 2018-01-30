'use strict';

const http = require('http');
const zlib = require('zlib');
const AWS = require('aws-sdk');

const isotrunc = require('./isotrunc.js');

module.exports = {};
module.exports.get = get;
module.exports.changes = changes;
module.exports.getGzipStream = getGzipStream;
module.exports.children = getChildren;

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

function getChildren(key) {
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

        const unit = isotrunc(key).unit();

        if (unit === 'minute' || unit === 'hour' || unit === 'day') {
            projExpression.push('#USERCOUNTS');
            xprsnAttrNames['#USERCOUNTS'] = 'userCounts';
        }

        dynamo.query({
            TableName: process.env.MainTable,
            Select: 'SPECIFIC_ATTRIBUTES',
            KeyConditionExpression: '#PARENT = :parent',
            ProjectionExpression: projExpression.join(', '),
            ExpressionAttributeNames: xprsnAttrNames,
            ExpressionAttributeValues: {
                ':parent': {S: key}
            }
        }, (err, data) => {
            if (err) return reject(err);
            if (data.Items.length === 0) {
                const error = new Error('no children for ' + key);
                error.name = 'NoChildren';
                return reject(error);
            }
            resolve(data.Items);
        });
    });
}
