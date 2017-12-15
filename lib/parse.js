'use strict';

const sax = require('sax');

module.exports = {};
module.exports.state = parseState;
module.exports.changes = parseChanges;

function parseState(stateText) {
    const state = {};

    stateText.split('\n')
        .filter(line => {
            return line.indexOf('=') > -1;
        }).forEach(line => {
            const split = line.split('=');
            state[split[0]] = split[1] || '';
        });

    const zeros = new Array(10).join('0');
    state.sequenceNumber = (zeros + state.sequenceNumber).slice(-9);

    const first = state.sequenceNumber.slice(0, 3);
    const second = state.sequenceNumber.slice(3, 6);
    const third = state.sequenceNumber.slice(6, 9);

    state.changeUrl = process.env.ReplicationPath +
        'minute/' + [first, second, third].join('/') + '.osc.gz';

    return Promise.resolve({
        state: state
    });
}

function parseChanges(readStream) {
    return new Promise((resolve, reject) => {
        const readableStream = readStream;
        const parser = sax.createStream(false, {
            position: false,
            lowercase: true
        });

        const stats = {};
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
                        'overallCounts': {
                            create_node: 0,
                            create_way: 0,
                            create_relation: 0,
                            modify_node: 0,
                            modify_way: 0,
                            modify_relation: 0,
                            delete_node: 0,
                            delete_way: 0,
                            delete_relation: 0
                        }
                    };
                }

                const user = node.attributes.user;
                if (!stats[minute].userCounts[user]) {
                    stats[minute].userCounts[user] = {
                        create_node: 0,
                        create_way: 0,
                        create_relation: 0,
                        modify_node: 0,
                        modify_way: 0,
                        modify_relation: 0,
                        delete_node: 0,
                        delete_way: 0,
                        delete_relation: 0
                    };
                }

                stats[minute].userCounts[user][action] += 1;

                if (!stats[minute].overallCounts[action])
                    stats[minute].overallCounts[action] = 0;

                stats[minute].overallCounts[action] += 1;
            }
        });

        parser.on('closetag', () => { depth -= 1; });
        parser.on('end', () => { resolve(stats); });
        parser.on('error', reject);

        readableStream.pipe(parser);
    });
}
