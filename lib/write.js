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

                let key = process.env.OutputPrefix + process.env.Environment + '/raw-stats/minutes/';
                key += minute + '-' + obj.state.sequenceNumber + '.csv.gz';

                s3.putObject({
                    Bucket: process.env.Bucket,
                    Key: key,
                    ACL: 'public-read',
                    Body: zlib.gzipSync(csv),
                    ContentType: 'text/csv',
                    ContentEncoding: 'gzip'
                }, (err) => {
                    if (err) return reject(err);
                    resolve(minute);
                });
            });

            promises.push(put);
        }

        return new Promise((resolve, reject) => {
            Promise.all(promises)
                .then((minutes) => {
                    const metrics = [{
                        MetricName: 'files_written',
                        Value: minutes.length,
                        Timestamp: new Date().toISOString()
                    }];

                    cw.putMetricData({
                        MetricData: metrics,
                        Namespace: ('osm-dash-' + process.env.Environment)
                    }, (err) => {
                        if (err) return reject(err);

                        let tempRollups = {};
                        minutes.forEach((minute) => {
                            let hour = minute.slice(0, 13);
                            tempRollups[hour] = {
                                type: 'hour',
                                datetime: hour
                            };

                            let day = minute.slice(0, 10);
                            tempRollups[day] = {
                                type: 'day',
                                datetime: day
                            };
                        });

                        let rollups = [];
                        for (const rollup in tempRollups) {
                            rollups.push(tempRollups[rollup]);
                        }

                        resolve(rollups);
                    });
                })
                .catch(reject);
        });
    }
};
