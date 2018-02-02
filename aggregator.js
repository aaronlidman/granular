'use strict';

const queue = require('./lib/queue.js');
const write = require('./lib/write.js');
const request = require('./lib/request.js');
const isotrunc = require('./lib/isotrunc.js');
const userCounts = require('./lib/userCounts.js');
const overallCounts = require('./lib/overallCounts.js');

exports.handler = (event, context, callback) => {
    event.callback = callback;
    processNext(event);
};

function processNext(context) {
    setContext(context)
        .then(getMessage)
        .then(getChildren)
        .then(userCounts.merge)
        .then(overallCounts.merge)
        .then(writeAggregate)
        .then(deleteMessage)
        .then(resetContext)
        .then(processNext)
        .catch(err => {
            if (err.name === 'NoChildren') {
                queue.hideMessageForFive({
                    QueueUrl: context.queue,
                    ReceiptHandle: context.message.ReceiptHandle
                }).then(() => resetContext(context))
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

function setContext(context) {
    if (!context.source) return Promise.reject(new Error('missing source'));

    context.queue = (context.source === 'tenMinTrigger') ?
        process.env.slowQueue : process.env.fastQueue;

    return Promise.resolve(context);
}

function getMessage(context) {
    return new Promise((resolve, reject) => {
        queue.receiveMessage({
            QueueUrl: context.queue,
            VisibilityTimeout: 10
        }).then(data => {
            console.log('starting', data.Body);
            context.message = JSON.parse(data.Body);
            context.message.ReceiptHandle = data.ReceiptHandle;
            context.message.MD5OfBody = data.MD5OfBody;
            context.message.key = isotrunc(context.message.key).to(context.message.jobType);
            resolve(context);
        }).catch(reject);
    });
}

function deleteMessage(context) {
    return new Promise((resolve, reject) => {
        queue.deleteMessage({
            QueueUrl: context.queue,
            ReceiptHandle: context.message.ReceiptHandle
        }, context.message.MD5OfBody).then(() => {
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
    return new Promise((resolve, reject) => {
        request.countItems(context.message.key)
            .then(children => {
                context.message.children = children;
                return context;
            })
            .then(resolve)
            .catch(reject);
    });
}

function writeAggregate(context) {
    const data = {
        overallCounts: context.message.data.overallCounts,
        userCounts: context.message.data.userCounts
    };

    // negative slices feel sketch
    const compositeKey = {
        parent: context.message.key.slice(0, -3),
        sequence: context.message.key.slice(-2)
    };

    return new Promise((resolve, reject) => {
        write.aggregate(compositeKey, data)
            .then(() => resolve(context))
            .catch(reject);
    });
}
