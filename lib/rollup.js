'use strict';

/*
    Gathers smaller files and rolls them into a larger one
*/

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const zlib = require('zlib');

function setVars(start, type) {
    var obj = {rollupType: type};

    if (type === 'hour') {
        obj.prior = 'minute';
        obj.length = 60;
        obj.start = start.slice(0, 13);
        obj.maxKeys = 60 * 5;
    } else if (type === 'day') {
        obj.prior = 'hours';
        obj.length = 1440;
        obj.start = start.slice(0, 10);
        obj.maxKeys = 24;
    } else {
        return false;
    }

    return Promise.resolve({context: obj});
}

function findFiles(obj) {
    return new Promise((resolve, reject) => {
        let prefix = process.env.OutputPrefix + process.env.Environment;
        prefix += '/raw-stats/' + obj.context.prior + 's/' + obj.context.start;

        s3.listObjectsV2({
            Bucket: process.env.Bucket,
            Prefix: prefix,
            MaxKeys: obj.context.maxKeys
        }, (err, data) => {
            if (err) return reject(err);
            resolve({
                context: obj.context,
                keys: data.Contents.map((file) => { return file.Key; })
            });
        });
    });
}

function getFiles(obj) {
    return new Promise((resolve, reject) => {
        var promises = [];

        obj.keys.map(key => {
            let get = new Promise((resolve, reject) => {
                const params = {
                    Bucket: process.env.Bucket,
                    Key: key
                };

                s3.getObject(params, (err, data) => {
                    if (err) return reject(err);
                    return resolve(zlib.gunzipSync(data.Body));
                });
            });

            return promises.push(get);
        });

        // they're all tiny so we load them up in memory for now
        // can stream them if we ever get to larger files

        Promise.all(promises)
            .then(stats => {
                return {
                    context: obj.context,
                    stats: stats
                };
            })
            .then(resolve)
            .catch(reject);
    });
}

function writeRollup(obj) {
    return new Promise((resolve, reject) => {

        let key = process.env.OutputPrefix + process.env.Environment + '/raw-stats/';
        key += obj.context.rollupType + 's/' + obj.context.start + '.csv.gz';

        s3.putObject({
            Bucket: process.env.Bucket,
            Key: key,
            ACL: 'public-read',
            Body: zlib.gzipSync(Buffer.concat(obj.stats)),
            ContentType: 'text/csv',
            ContentEncoding: 'gzip'
        }, (err, data) => {
            if (err) return reject(err);
            resolve(data);
        });
    });
}

module.exports = (start, type) => {
    return new Promise((resolve, reject) => {
        setVars(start, type)
            .then(findFiles)
            .then(getFiles)
            .then(writeRollup)
            .then(resolve)
            .catch(reject);
    });
};
