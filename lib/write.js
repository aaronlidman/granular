'use strict';

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const zlib = require('zlib');

const config = require('../config.json');

module.exports = {
    overallFile: function (stats) {
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

        return new Promise((resolve, reject) => {
            s3.putObject({
                Bucket: config.bucket,
                Key: config.prefix + process.env.Environment + '/overall/allUsers-' + +new Date() + '.csv.gz',
                ACL: 'public-read',
                Body: zlib.gzipSync(header + csv),
                ContentType: 'text/csv',
                ContentEncoding: 'gzip'
            }, (err, data) => {
                if (err) return reject(err);
                resolve(data);
            });
        });
    }
};
