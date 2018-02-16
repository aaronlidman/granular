'use strict';

const AWS = require('aws-sdk');

const isotrunc = require('./isotrunc.js');
const skipList = {};

module.exports = {};
module.exports.sendMessage = sendMessage;
module.exports.receiveMessage = receiveMessage;
module.exports.deleteMessage = deleteMessage;
module.exports.minuteAggregation = minuteAggregation;
module.exports.skipList = skipList;
module.exports.hideMessageForFive = hideMessageForFive;

function sendMessage(queue, body) {
    const sqs = new AWS.SQS({region: process.env.AWS_REGION || 'us-west-2'});

    return new Promise((resolve, reject) => {
        sqs.sendMessage({
            QueueUrl: queue,
            MessageBody: JSON.stringify(body)
        }, (err, data) => {
            if (err) return reject(err);
            resolve(data);
        });
    });
}

function receiveMessage(params) {
    const sqs = new AWS.SQS({region: process.env.AWS_REGION || 'us-west-2'});

    return new Promise((resolve, reject) => {
        sqs.receiveMessage(Object.assign(params, {
            AttributeNames: ['SentTimestamp'],
            VisibilityTimeout: 10
        }), (err, data) => {
            if (err) return reject(err);
            if (!data.Messages || data.Messages.length === 0) {
                const error = new Error('no messages in queue');
                error.name = 'NoMoMessages';
                return reject(error);
            }

            const message = data.Messages[0];

            // skip and delete message if contents match one we've seen and processed recently
            const msgCreated = parseInt(message.Attributes.SentTimestamp);
            if ((message.MD5OfBody in skipList) &&
                (msgCreated < skipList[message.MD5OfBody])) {

                console.log('skipping', message.Body, 'completed it recently');

                deleteMessage({
                    QueueUrl: params.QueueUrl,
                    ReceiptHandle: message.ReceiptHandle
                }, message.MD5OfBody).then(() => {
                    receiveMessage(params)
                        .then(resolve)
                        .catch(reject);
                });
            } else {
                resolve(message);
            }
        });
    });
}

function deleteMessage(params, MD5OfBody) {
    const sqs = new AWS.SQS({region: process.env.AWS_REGION || 'us-west-2'});

    return new Promise((resolve, reject) => {
        sqs.deleteMessage(params, (err, data) => {
            if (err) return reject(err);
            addToSkipList(MD5OfBody);
            resolve(data);
        });
    });
}

function hideMessageForFive(params) {
    const sqs = new AWS.SQS({region: process.env.AWS_REGION || 'us-west-2'});

    return new Promise((resolve, reject) => {
        sqs.changeMessageVisibility({
            QueueUrl: params.QueueUrl,
            ReceiptHandle: params.ReceiptHandle,
            VisibilityTimeout: 5 * 60
        }, (err, data) => {
            if (err) return reject(err);
            resolve(data);
        });
    });
}

function minuteAggregation(keys, propogate) {
    const promises = [];

    keys.forEach(key => {
        const message = {
            worker: 'aggregator',
            jobType: 'minute',
            key: isotrunc(key).to('minute')
        };
        promises.push(sendMessage(process.env.fastQueue, message));
    });

    if (propogate) {
        promises.push(hourAggregation(keys));
        promises.push(dayAggregation(keys));
        // promises.push(monthAggregation);
    }

    return Promise.all(promises);
}

function hourAggregation(keys) {
    const promises = [];

    // remove unnecessary overlaps
    keys = Array.from(new Set(keys.map(key => isotrunc(key).to('hour'))));

    keys.forEach(key => {
        const message = {
            worker: 'aggregator',
            jobType: 'hour',
            key: key
        };
        promises.push(sendMessage(process.env.fastQueue, message));
    });

    return Promise.all(promises);
}

function dayAggregation(keys) {
    const promises = [];

    // remove unnecessary overlaps
    keys = Array.from(new Set(keys.map(key => isotrunc(key).to('day'))));

    keys.forEach(key => {
        const message = {
            worker: 'aggregator',
            jobType: 'day',
            key: key
        };
        promises.push(sendMessage(process.env.slowQueue, message));
    });

    return Promise.all(promises);
}

function addToSkipList(key, offset) {
    if (offset === undefined) offset = 10 * 1000;
    if (!key) throw new Error('invalid key');
    skipList[key] = +new Date() + offset;
}
