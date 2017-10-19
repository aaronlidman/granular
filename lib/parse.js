'use strict';

const sax = require('sax');
const config = require('../config.json');

module.exports = {
    state: function (stateText) {
        let state = {};

        stateText.split('\n')
            .filter((line) => {
                return line.indexOf('=') > -1;
            })
            .forEach((line) => {
                let split = line.split('=');
                state[split[0]] = split[1] || '';
            });

        const zeros = new Array(10).join('0');
        state.sequenceNumber = (zeros + state.sequenceNumber).slice(-9);

        let first = state.sequenceNumber.slice(0, 3);
        let second = state.sequenceNumber.slice(3, 6);
        let third = state.sequenceNumber.slice(6, 9);

        const changeUrl = config.base_url +
            config.replication_dir +
            [first, second, third].join('/') + '.osc.gz';

        return Promise.resolve({
            state: state,
            changeUrl: changeUrl
        });
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
                const user = node.attributes.user.split(',').join('.');
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

        readableStream.pipe(parser);

        return new Promise((resolve) => {
            parser.on('end', () => {
                resolve(stats);
            });
        });
    }
};
