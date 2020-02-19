const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = async (client, from, to, callback) => {
    const res = await client.count({
        index: indexName,
        body: {
            query: {
                range: {
                    "@timestamp": {
                        format: "yyyy-MM-dd",
                        gte: from,
                        lte: to
                    }
                }
            }
        }
    });
    callback({
        count: res.body.count
    })
}

exports.countAround = async (client, lat, lon, radius, callback) => {
    const res = await client.count({
        index: indexName,
        body: {
            query: {
                bool: {
                    must: {
                        match_all: {}
                    },
                    filter: {
                        geo_distance: {
                            distance : radius,
                            location: {
                                lat, 
                                lon
                            }
                        }
                    }
                }
            }
        }
    });
    callback({
        count: res.body.count
    })
}