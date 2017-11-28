'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');
const region = process.env.AWS_REGION || 'us-west-2';
const dynamo = new AWS.DynamoDB({region: region});
const sqs = new AWS.SQS({region: region});

const userCountsLib = require('./lib/userCounts');
const overallCountsLib = require('./lib/overallCounts');

exports.handler = (event, context, callback) => {
    processNext(event.detail, callback);
};

function processNext(context, callback) {
    getMessage(context)
        .then(getChildren)
        .then(mergeChildren)
        .then(packageValues)
        .then(writeToDynamo)
        .then(deleteMessage)
        .then(() => {
            processNext(context, callback);
        })
        .catch((err) => {
            if (err.no_messages) {
                // retries or concurrency checks here in the future
                return callback(null);
            } else {
                err.context = context;
                console.error(err);
                return callback(err);
            }
        });
}

function getMessage(context) {
    return new Promise((resolve, reject) => {
        if (context.source) {
            if (context.source === 'trigger.perTenMin') {
                context.queue = process.env.perTenMinQueue;
            } else {
                context.queue = process.env.perMinQueue;
            }
        } else {
            return reject(new Error('missing source'));
        }

        sqs.receiveMessage({
            QueueUrl: context.queue,
            VisibilityTimeout: 10,
            WaitTimeSeconds: 20
        }, (err, data) => {
            if (err) return reject(err);
            if (!data.Messages) return reject({no_messages: true});

            context.job = JSON.parse(data.Messages[0].Body);
            context.job.ReceiptHandle = data.Messages[0].ReceiptHandle;

            resolve(context);
        });
    });
}

function deleteMessage(context) {
    return new Promise((resolve, reject) => {
        sqs.deleteMessage({
            QueueUrl: context.queue,
            ReceiptHandle: context.job.ReceiptHandle
        }, (err, data) => {
            if (err) return reject(err);
            delete context.job;
            resolve(context);
        });
    });
}

function getChildren(context) {
    // get all the children for the given parent

    return new Promise((resolve, reject) => {
        // all jobs want these things for now, that might change
        const projExpression = ['#SEQUENCE', '#OVERALLCOUNTS'];
        const xprsnAttrNames = {
            '#PARENT': 'parent',
            '#OVERALLCOUNTS': 'overallCounts',
            '#SEQUENCE': 'sequence'
        };

        if (context.job.jobType === 'minute' ||
            context.job.jobType === 'hour') {
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
                ':parent': {S: context.job.key}
            }
        }, (err, data) => {
            if (err) return reject(err);
            context.job.children = data.Items;
            resolve(context);
        });
    });
}

function mergeChildren(context) {
    // merges children of various formats

    if (context.job.jobType === 'minute') {
        let userCounts = userCountsLib.mergeTotals(context.job.children);
        userCounts = userCountsLib.collapseTotals(userCounts);

        let overallCounts = overallCountsLib.mergeTotals(context.job.children);
        overallCounts = overallCountsLib.collapseTotals(overallCounts);

        context.job.data = {
            userCounts: userCounts,
            overallCounts: overallCounts
        };
    }

    if (context.job.jobType === 'hour') {
        // put totals into the appropriate position in an array
            // use the sequence
        // input: array of individual objects holding totals
        // output: sparse array of totals
    }

    delete context.job.children;
    return Promise.resolve(context);
}

function packageValues(context) {
    // pack up data for dynamo

    if (context.job.data.userCounts) {
        context.job.data.userCounts = {
            B: zlib.gzipSync(JSON.stringify(context.job.data.userCounts))
        };
    }

    if (context.job.data.overallCounts) {
        context.job.data.overallCounts = {
            B: zlib.gzipSync(JSON.stringify(context.job.data.overallCounts))
        };
    }

    return context;
}

function writeToDynamo(context) {
    // dynamically write all data

    return new Promise((resolve, reject) => {
        const attrs = [];
        for (const attr in context.job.data) {
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
            attrValues[subValue] = context.job.data[attr];
        });

        // come up with a better way of modifying parent and sequence length
        // like actually using Date(), negative slices feel sketch
        dynamo.updateItem({
            TableName: process.env.MainTable,
            Key: {
                parent: {S: context.job.key.slice(0, -3)},
                sequence: {N: context.job.key.slice(-2)}
            },
            UpdateExpression: 'SET ' + updateExpression.join(', '),
            ExpressionAttributeNames: attrNames,
            ExpressionAttributeValues: attrValues
        }, (err, data) => {
            if (err) return reject(err);
            resolve(context);
        });
    });
}
