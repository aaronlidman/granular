'use strict';

const AWS = require('aws-sdk');

module.exports = {};
module.exports.generic = generic;
module.exports.minuteAggregation = minuteAggregation;

function generic(queue, body) {
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

function minuteAggregation(keys, propogate) {
    const promises = [];

    keys.forEach(key => {
        promises.push(generic(process.env.perMinQueue, {
            worker: 'aggregator',
            jobType: 'minute',
            key: key.slice(0, 16)
        }));
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
        const promise = generic(process.env.perMinQueue, message, 5);
        promises.push(promise);
    });

    return Promise.all(promises);
}

function dayAggregation(keys) {
    const promises = [];

    keys.forEach(key => {
        promises.push(generic(process.env.perTenMinQueue, {
            worker: 'aggregator',
            jobType: 'day',
            key: key.slice(0, 10)
        }));
    });

    return Promise.all(promises);
}
