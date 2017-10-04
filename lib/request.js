'use strict';

const http = require('http');
const zlib = require('zlib');

module.exports = {
    get: function (url) {
        return new Promise((resolve, reject) => {
            const req = http.get(url, (res) => {
                if (res.statusCode !== 200) {
                    reject(new Error('statusCode:' + res.statusCode));
                }

                let allData = '';

                res.on('data', (chunk) => {
                    allData += chunk;
                });

                res.on('end', () => {
                    resolve(allData);
                });
            });

            req.on('error', (err) => reject(err));
        });
    },
    getStream: function (url) {
        const request = http.get(url);

        return new Promise((resolve, reject) => {
            request.on('response', (response) => {
                resolve(response.pipe(zlib.createGunzip()));
            });

            request.on('error', reject);
        });
    }
};
