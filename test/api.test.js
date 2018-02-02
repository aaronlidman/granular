'use strict';

const test = require('tape');
const api = require('../api.js');
const AWS = require('aws-sdk-mock');

test('400 response', t => {
    api.handler({}, {}, (error, response) => {
        t.deepEqual(error, {
            statusCode: 400,
            headers: {},
            isBase64Encoded: false,
            body: 'must specify a time, eg ?time=2018-01-01T01:01'
        });
        t.equal(response, undefined);
        t.end();
    });
});

test('fetch and return item', t => {
    AWS.mock('DynamoDB', 'query', function (params, callback) {
        const data = {Items: []};

        if (params.ExpressionAttributeValues[':parent'].S === '2018-02-11T12' &&
            params.ExpressionAttributeValues[':sequence'].N === '10') {
            // just the structure, intentionally no data
            data.Items = [{'overallCounts': {'B': {'type': 'Buffer', 'data': []}}, 'sequence': {'N': '10'}}];
        }

        callback(null, data);
    });

    const time = new Date('2018-02-11T12:10').toISOString();

    const event = {
        queryStringParameters: {
            time: time
        }
    };

    process.env.MainTable = 'MainTable';

    api.handler(event, {}, (error, response) => {
        t.equal(error, null);
        t.deepEqual(response, {
            statusCode: 200,
            headers: {},
            isBase64Encoded: false,
            body: '[{"overallCounts":{"B":{"type":"Buffer","data":[]}},"sequence":{"N":"10"}}]'
        });

        AWS.restore('DynamoDB');
        t.end();
    });
});

test('fetch and return item', t => {
    AWS.mock('DynamoDB', 'query', function (params, callback) {
        const data = {Items: []};
        callback(null, data);
    });

    const time = new Date('2020-02-11T12:10').toISOString();

    const event = {
        queryStringParameters: {
            time: time
        }
    };

    process.env.MainTable = 'MainTable';

    api.handler(event, {}, (error, response) => {
        t.equal(error, null);
        t.deepEqual(response, {
            statusCode: 200,
            headers: {},
            isBase64Encoded: false,
            body: '[]'
        });

        AWS.restore('DynamoDB');
        t.end();
    });
});
