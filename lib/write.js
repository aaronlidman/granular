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
            const stats = obj.stats[minute];
            const header = 'time,userc_nodes,m_nodes,d_nodes,c_ways,m_ways,d_ways,c_relations,m_relations,d_relations\n';
            let csv = '';

            for (const user in stats) {
                csv += [minute,
                    user,
                    (stats[user].c_node || 0),
                    (stats[user].m_node || 0),
                    (stats[user].d_node || 0),
                    (stats[user].c_way || 0),
                    (stats[user].m_way || 0),
                    (stats[user].d_way || 0),
                    (stats[user].c_relation || 0),
                    (stats[user].m_relation || 0),
                    (stats[user].d_relation || 0)
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
                    Body: zlib.gzipSync(header + csv),
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
