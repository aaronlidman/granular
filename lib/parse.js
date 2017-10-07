'use strict';

const sax = require('sax');
const config = require('../config.json');

module.exports = {
    state: function (stateText) {
        let sequenceNum = stateText.split('\n').filter((line) => {
            return line.indexOf('sequenceNumber') > -1;
        });

        sequenceNum = sequenceNum[0].split('=')[1];
        const zeros = new Array(10).join('0');
        sequenceNum = (zeros + sequenceNum).slice(-9);

        let first = sequenceNum.slice(0, 3);
        let second = sequenceNum.slice(3, 6);
        let third = sequenceNum.slice(6, 9);

        const changeUrl = config.base_url +
            config.replication_dir +
            [first, second, third].join('/') + '.osc.gz';

        return Promise.resolve(changeUrl);
    },
    change: function (readableStream) {
        const parser = sax.createStream(false, {
            position: false,
            lowercase: true
        });

        let stats = {
            '_overall': {}
        };
        // keyed by username
        // overall stats are simply a special username, '_overall'

        let action;
        let depth = 0;

        parser.on('opentag', (node) => {
            depth += 1;

            if (depth === 2) {
                action = node.name[0];
            } else if (depth === 3) {
                const user = node.attributes.user;
                if (!stats[user]) stats[user] = {};

                if (stats[user][action + node.name]) {
                    stats[user][action + node.name] += 1;
                } else {
                    stats[user][action + node.name] = 1;
                }

                if (stats['_overall'][action + node.name]) {
                    stats['_overall'][action + node.name] += 1;
                } else {
                    stats['_overall'][action + node.name] = 1;
                }
            }

            // use other depths for refs and such in the future
        });

        parser.on('closetag', () => {
            depth -= 1;
        });

        parser.on('end', () => {
            return Promise.resolve(stats);
        });

        readableStream.pipe(parser);
    }
};
