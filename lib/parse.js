'use strict';

const sax = require('sax');

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

        state.changeUrl = process.env.ReplicationPath +
            'minute/' + [first, second, third].join('/') + '.osc.gz';

        return Promise.resolve({
            state: state
        });
    },
    changes: function (obj) {
        const readableStream = obj.changes;
        const parser = sax.createStream(false, {
            position: false,
            lowercase: true
        });

        let stats = {
            '_overall': {}
        };
        // keyed by username
        // overall stats are simply a special username, '_overall'

        let times = {};

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

                const minute = node.attributes.timestamp.slice(0, 16);
                if (!times[minute]) times[minute] = 0;
                times[minute] += 1;
            }

            // use other depths for refs and such in the future
        });

        parser.on('closetag', () => {
            depth -= 1;
        });

        readableStream.pipe(parser);

        return new Promise((resolve, reject) => {
            parser.on('error', reject);

            parser.on('end', () => {
                // find the dominant minute
                let minute = Object.keys(times).reduce((a, b) => {
                    return times[a] > times[b] ? a : b;
                });

                resolve({
                    state: obj.state,
                    stats: stats,
                    time: minute
                });
            });
        });
    }
};
