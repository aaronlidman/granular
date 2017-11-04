'use strict';

const zlib = require('zlib');
const AWS = require('aws-sdk');

module.exports = {
    minutelyStats: (obj) => {
        const s3 = new AWS.S3();
        const cw = new AWS.CloudWatch({
            region: process.env.AWS_REGION || 'us-west-2'
        });
        // aws services required in here for test mocking

        let promises = [];

        for (const minute in obj.stats) {
            const stats = obj.stats[minute];
            let csv = '';

            for (const user in stats) {
                if (user === '_overall') return;
                csv += [minute,
                    user,
                    stats[user].c_node,
                    stats[user].m_node,
                    stats[user].d_node,
                    stats[user].c_way,
                    stats[user].m_way,
                    stats[user].d_way,
                    stats[user].c_relation,
                    stats[user].m_relation,
                    stats[user].d_relation
                ].join(',') + '\n';
            }

            const put = new Promise((resolve, reject) => {
                if (!obj.state || !obj.state.sequenceNumber) return reject('missing sequenceNumber');

                let time = minute.slice(0, -4);
                let key = process.env.OutputPrefix + process.env.Environment + '/raw-stats/minutes/';
                key += time + '-' + obj.state.sequenceNumber + '.csv.gz';

                s3.putObject({
                    Bucket: process.env.Bucket,
                    Key: key,
                    ACL: 'public-read',
                    Body: zlib.gzipSync(csv),
                    ContentType: 'text/csv',
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
