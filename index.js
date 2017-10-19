'use strict';

const config = require('./config.json');
const parse = require('./lib/parse.js');
const request = require('./lib/request.js');
const cwput = require('./lib/cwput.js');

module.exports.handler = function () {
    request.get(config.base_url + config.replication_dir + 'state.txt')
        .then(parse.state)
        .then(request.getGzipStream)
        .then(parse.change)
        .then(cwput.overallMetrics)
        .then((stats) => {
            console.log('user,c_nodes,m_nodes,d_nodes,c_ways,m_ways,d_ways,c_relations,m_relations,d_relations');
            for (const user in stats) {
                console.log([user,
                    (stats[user].cnode || 0),
                    (stats[user].mnode || 0),
                    (stats[user].dnode || 0),
                    (stats[user].cway || 0),
                    (stats[user].mway || 0),
                    (stats[user].dway || 0),
                    (stats[user].crelation || 0),
                    (stats[user].mrelation || 0),
                    (stats[user].drelation || 0)
                ].join(','));
            }
        })
        .catch((err) => console.log(err));
};
