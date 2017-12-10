'use strict';

const AWS = require('aws-sdk');

module.exports = {};
module.exports.sendMessage = sendMessage;
module.exports.receiveMessage = receiveMessage;
module.exports.minuteAggregation = minuteAggregation;

function sendMessage(queue, body) {
    const sqs = new AWS.SQS({region: process.env.AWS_REGION || 'us-west-2'});

    return new Promise((resolve, reject) => {
        sqs.sendMessage({
            QueueUrl: queue,
            MessageBody: JSON.stringify(body),
            MessageGroupId: body.jobType
        }, (err, data) => {
            if (err) return reject(err);
            resolve(data);
        });
    });
}

function receiveMessage(params) {
    const sqs = new AWS.SQS({region: process.env.AWS_REGION || 'us-west-2'});

    return new Promise((resolve, reject) => {
        sqs.receiveMessage(params, (err, data) => {
            if (err) return reject(err);
            if (!data.Messages || data.Messages.length === 0) return reject(new Error('no messages in queue'));

            resolve(data.Messages[0]);
        });
    });
}

function minuteAggregation(keys, propogate) {
    const promises = [];

    keys.forEach(key => {
        const message = {
            worker: 'aggregator',
            jobType: 'minute',
            key: key.slice(0, 16)
        };
        promises.push(sendMessage(process.env.perMinQueue, message));
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

    keys.forEach(key => {
        const message = {
            worker: 'aggregator',
            jobType: 'hour',
            key: key.slice(0, 13)
        };
        promises.push(sendMessage(process.env.perMinQueue, message));
    });

    return Promise.all(promises);
}

function dayAggregation(keys) {
    const promises = [];

    keys.forEach(key => {
        const message = {
            worker: 'aggregator',
            jobType: 'day',
            key: key.slice(0, 10)
        };
        promises.push(sendMessage(process.env.perTenMinQueue, message));
    });

    return Promise.all(promises);
}
