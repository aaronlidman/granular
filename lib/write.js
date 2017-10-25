'use strict';

const zlib = require('zlib');
const AWS = require('aws-sdk');

module.exports = {
    minutelyStats: function (obj) {
        const s3 = new AWS.S3();
        const cw = new AWS.CloudWatch({
            region: process.env.AWS_REGION || 'us-west-2'
        });
        // required in here for test mocking

        let promises = [];

        for (const minute in obj.stats) {
            const stats = obj.stats[minute];
            const header = 'user,c_nodes,m_nodes,d_nodes,c_ways,m_ways,d_ways,c_relations,m_relations,d_relations\n';
            let csv = '';

            for (const user in stats) {
                csv += [user,
                    (stats[user].cnode || 0),
                    (stats[user].mnode || 0),
                    (stats[user].dnode || 0),
                    (stats[user].cway || 0),
                    (stats[user].mway || 0),
                    (stats[user].dway || 0),
                    (stats[user].crelation || 0),
                    (stats[user].mrelation || 0),
                    (stats[user].drelation || 0)
                ].join(',') + '\n';
            }

            const put = new Promise((resolve, reject) => {
                if (!obj.state || !obj.state.sequenceNumber) return reject('missing sequenceNumber');

                let key = process.env.OutputPrefix + process.env.Environment + '/raw-stats/';
                key += minute.slice(0, -4) + '-' + obj.state.sequenceNumber + '.csv.gz';

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
