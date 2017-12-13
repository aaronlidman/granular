'use strict';

const http = require('http');
const zlib = require('zlib');

module.exports = {};
module.exports.get = get;
module.exports.changes = changes;
module.exports.getGzipStream = getGzipStream;

function get(url) {
    return new Promise((resolve, reject) => {
        const request = http.get(url, (response) => {
            if (response.statusCode !== 200) {
                reject(new Error('statusCode: ' + response.statusCode));
            }

            let allData = '';
            response.on('data', chunk => {
                allData += chunk;
            });

            response.on('end', () => {
                resolve(allData);
            });
        });

        request.on('error', reject);
    });
}

function changes(obj) {
    return new Promise((resolve, reject) => {
        getGzipStream(obj.state.changeUrl)
            .then(readStream => {
                resolve({
                    state: obj.state,
                    changes: readStream
                });
            })
            .catch(reject);
    });
}

function getGzipStream(url) {
    const request = http.get(url);

    return new Promise((resolve, reject) => {
        request.on('response', response => {
            resolve(response.pipe(zlib.createGunzip()));
        });

        request.on('error', reject);
    });
}
