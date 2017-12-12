'use strict';

const AWS = require('aws-sdk');
const region = process.env.AWS_REGION || 'us-west-2';
const dynamo = new AWS.DynamoDB({region: region});

const queue = require('./lib/queue.js');
const userCounts = require('./lib/userCounts.js');
const overallCounts = require('./lib/overallCounts.js');

exports.handler = (event, context, callback) => {
    event.callback = callback;
    processNext(event);
};

function processNext(context) {
    getMessage(context)
        .then(getChildren)
        .then(userCounts.merge)
        .then(userCounts.pack)
        .then(overallCounts.merge)
        .then(overallCounts.pack)
        .then(writeToDynamo)
        .then(deleteMessage)
        .then(resetContext)
        .then(processNext)
        .catch((err) => {
            if (err.name === 'NoChildren') {
                const hideParams = {
                    QueueUrl: context.queue,
                    ReceiptHandle: context.message.ReceiptHandle
                };

                queue.hideMessageForFive(hideParams)
                    .then(() => resetContext(context))
                    .then(processNext)
                    .catch(err => errorAndExit(context, err));
            } else if (err.name === 'NoMoMessages') {
                context.callback(null);
            } else {
                return errorAndExit(context, err);
            }
        });
}

function errorAndExit(context, err) {
    console.error(err.stack, context);
    return context.callback(err);
}

function getMessage(context) {
    return new Promise((resolve, reject) => {
        if (context.source) {
            if (context.source === 'perTenMinTrigger') {
                context.queue = process.env.slowQueue;
            } else {
                context.queue = process.env.fastQueue;
            }
        } else {
            return reject(new Error('missing source'));
        }

        queue.receiveMessage({
            QueueUrl: context.queue,
            VisibilityTimeout: 10
        }).then(data => {
            console.log('starting', data.Body);
            context.message = JSON.parse(data.Body);
            context.message.ReceiptHandle = data.ReceiptHandle;
            context.message.MD5OfBody = data.MD5OfBody;

            // validate the key real quick, this should be a lib
            if (context.message.jobType === 'minute') context.message.key = context.message.key.slice(0, 16);
            if (context.message.jobType === 'hour') context.message.key = context.message.key.slice(0, 13);
            if (context.message.jobType === 'day') context.message.key = context.message.key.slice(0, 10);
            if (context.message.jobType === 'month') context.message.key = context.message.key.slice(0, 7);

            resolve(context);
        }).catch(reject);
    });
}

function deleteMessage(context) {
    return new Promise((resolve, reject) => {
        const params = {
            QueueUrl: context.queue,
            ReceiptHandle: context.message.ReceiptHandle
        };

        queue.deleteMessage(params, context.message.MD5OfBody).then(() => {
            console.log('finished', JSON.stringify({
                key: context.message.key,
                jobType: context.message.jobType
            }));
            resolve(context);
        }).catch(reject);
    });
}

function resetContext(context) {
    delete context.message;
    return Promise.resolve(context);
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

        if (context.message.jobType === 'minute' ||
            context.message.jobType === 'hour' ||
            context.message.jobType === 'day') {
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
                ':parent': {S: context.message.key}
            }
        }, (err, data) => {
            if (err) return reject(err);
            if (data.Items.length === 0) {
                const error = new Error('no children for ' + context.message.key);
                error.name = 'NoChildren';
                return reject(error);
            }
            context.message.children = data.Items;
            resolve(context);
        });
    });
}

function writeToDynamo(context) {
    // dynamically write all data

    return new Promise((resolve, reject) => {
        const attrs = [];
        for (const attr in context.message.data) {
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
            attrValues[subValue] = context.message.data[attr];
        });

        // come up with a better way of modifying parent and sequence length
        // like actually using Date(), negative slices feel sketch
        dynamo.updateItem({
            TableName: process.env.MainTable,
            Key: {
                parent: {S: context.message.key.slice(0, -3)},
                sequence: {N: context.message.key.slice(-2)}
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
