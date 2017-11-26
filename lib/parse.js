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
        return new Promise((resolve, reject) => {
            const readableStream = obj.changes;
            const parser = sax.createStream(false, {
                position: false,
                lowercase: true
            });

            let stats = {};
            let edit;
            let depth = 0;

            parser.on('opentag', node => {
                depth += 1;

                if (depth === 2) {
                    edit = node.name;
                } else if (depth === 3) {
                    const minute = node.attributes.timestamp.slice(0, 16);
                    const action = edit + '_' + node.name;

                    if (!stats[minute]) {
                        stats[minute] = {
                            'userCounts': {},
                            'overallCounts': {}
                        };
                    }

                    const user = node.attributes.user;
                    if (!stats[minute].userCounts[user]) stats[minute].userCounts[user] = {};

                    if (stats[minute].userCounts[user][action]) {
                        stats[minute].userCounts[user][action] += 1;
                    } else {
                        stats[minute].userCounts[user][action] = 1;
                    }

                    if (stats[minute].overallCounts[action]) {
                        stats[minute].overallCounts[action] += 1;
                    } else {
                        stats[minute].overallCounts[action] = 1;
                    }
                }
            });

            parser.on('closetag', () => {
                depth -= 1;
            });

            readableStream.pipe(parser);

            parser.on('error', reject);

            parser.on('end', () => {
                let resolved = {stats: stats};
                if (obj.state) resolved.state = obj.state;
                resolve(resolved);
            });
        });
    }
};
