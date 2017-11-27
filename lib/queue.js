'use strict';

const AWS = require('aws-sdk');

module.exports = {};
module.exports.generic = generic;
module.exports.minuteAggregation = minuteAggregation;

function generic(queue, body) {
    const sqs = new AWS.SQS();

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

function minuteAggregation(keys) {
    const promises = [];

    keys.forEach(key => {
        promises.push(generic(process.env.perMinQueue, {
            worker: 'aggregator',
            jobType: 'minute',
            key: key
        }));
    });

    return Promise.all(promises);
}
