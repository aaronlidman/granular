'use strict';

const zlib = require('zlib');
const AWS = require('aws-sdk');

module.exports = {
    minutelyStats: function (obj) {
        const s3 = new AWS.S3();
        const cw = new AWS.CloudWatch({
            region: process.env.AWS_REGION || 'us-west-2'
        });
        // aws services required in here for test mocking

        let promises = [];

        for (const minute in obj.stats) {
            const put = new Promise((resolve, reject) => {
                if (!obj.state || !obj.state.sequenceNumber) return reject('missing sequenceNumber');

                let key = process.env.OutputPrefix + process.env.Environment + '/raw-stats/minutes/';
                key += minute.slice(0, -4) + '-' + obj.state.sequenceNumber + '.json.gz';

                s3.putObject({
                    Bucket: process.env.Bucket,
                    Key: key,
                    ACL: 'public-read',
                    Body: zlib.gzipSync(JSON.stringify(obj.stats[minute])),
                    ContentType: 'application/json',
                    ContentEncoding: 'gzip'
                }, (err, data) => {
                    if (err) return reject(err);
                    resolve(data);
                });
            });

            promises.push(put);
        }

        return new Promise((resolve, reject) => {
            Promise.all(promises)
                .then((data) => {
                    const metrics = [{
                        MetricName: 'files_written',
                        Value: data.length,
                        Timestamp: new Date().toISOString()
                    }];

                    cw.putMetricData({
                        MetricData: metrics,
                        Namespace: ('osm-dash-' + process.env.Environment)
                    }, (err) => {
                        if (err) return reject(err);
                        resolve(data);
                    });
                })
                .catch(reject);
        });
    }
};
