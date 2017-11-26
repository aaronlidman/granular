'use strict';

const AWS = require('aws-sdk');

module.exports = {};
module.exports.generic = generic;
module.exports.sequence = sequence;

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

function sequence(key) {
    return generic(process.env.perMinQueue, {
        worker: 'aggregator',
        jobType: 'sequence',
        key: key
    });
}
