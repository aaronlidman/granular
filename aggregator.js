'use strict';

const AWS = require('aws-sdk');
const region = process.env.AWS_REGION || 'us-west-2';
const dynamo = new AWS.DynamoDB({region: region});
const sqs = new AWS.SQS({region: region});

const userCounts = require('./lib/userCounts.js');
const overallCounts = require('./lib/overallCounts.js');

exports.handler = (event, context, callback) => {
    processNext(event, callback);
};

function processNext(context, callback) {
    getJob(context)
        .then(getChildren)
        .then(userCounts.merge)
        .then(userCounts.pack)
        .then(overallCounts.merge)
        .then(overallCounts.pack)
        .then(writeToDynamo)
        .then(markJobDone)
        .then(() => {
            processNext(context, callback);
        }).catch((err) => {
            if (err.no_messages) {
                // retries or concurrency checks here in the future
                return callback(null);
            } else {
                console.error(err.stack, context);
                return callback(err);
            }
        });
}

function getJob(context) {
    return new Promise((resolve, reject) => {
        if (context.source) {
            if (context.source === 'perTenMinTrigger') {
                context.queue = process.env.perTenMinQueue;
            } else {
                context.queue = process.env.perMinQueue;
            }
        } else {
            return reject(new Error('missing source'));
        }

        sqs.receiveMessage({
            QueueUrl: context.queue,
            VisibilityTimeout: 10
        }, (err, data) => {
            if (err) return reject(err);
            if (!data.Messages) return reject({no_messages: true});

            console.log('starting', data.Messages[0].Body);
            context.job = JSON.parse(data.Messages[0].Body);
            context.job.ReceiptHandle = data.Messages[0].ReceiptHandle;

            // validate the key real quick, this should be a lib
            if (context.job.jobType === 'minute') context.job.key = context.job.key.slice(0, 16);
            if (context.job.jobType === 'hour') context.job.key = context.job.key.slice(0, 13);
            if (context.job.jobType === 'day') context.job.key = context.job.key.slice(0, 10);
            if (context.job.jobType === 'month') context.job.key = context.job.key.slice(0, 7);

            resolve(context);
        });
    });
}

function markJobDone(context) {
    return new Promise((resolve, reject) => {
        sqs.deleteMessage({
            QueueUrl: context.queue,
            ReceiptHandle: context.job.ReceiptHandle
        }, (err, data) => {
            if (err) return reject(err);

            delete context.job.data;
            delete context.job.children;

            console.log('finished', JSON.stringify(context.job));

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
            context.job.jobType === 'hour' ||
            context.job.jobType === 'day') {
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
            if (data.Items.length === 0) return reject(new Error('no children for ' + context.job.key));
            context.job.children = data.Items;
            resolve(context);
        });
    });
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
