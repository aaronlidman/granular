'use strict';

const AWS = require('aws-sdk');
const crypto = require('crypto');

module.exports = {
    rollups: function (arr) {
        var sqs = new AWS.SQS({
            region: process.env.AWS_REGION || 'us-west-2'
        });

        return new Promise((resolve, reject) => {
            // batch size is limited to 10
            sqs.sendMessageBatch({
                QueueUrl: process.env.RollupQueue,
                Entries: arr.map(message => {
                    const msg = JSON.stringify(message);
                    return {
                        Id: crypto.createHash('sha1').update(msg).digest('hex'),
                        MessageBody: msg
                    };
                })
            }, (err, data) => {
                if (err) return reject(err);
                resolve(data);
            });
        });
    }
};
