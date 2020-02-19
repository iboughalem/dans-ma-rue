const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = async (client, callback) => {
    const response = await client.search({
        size: 0,
        index: indexName,
        body: {
            aggs: {
                arrondissements: {
                    terms: {
                        field: "arrondissement.keyword",
                        order: { _count: "desc" },
                        size: 20
                    }
                }
            }
        }
    });
    const res = response.body.aggregations.arrondissements.buckets.map(element => {
        return {
            "arrondissement": element.key,
            "count": element.doc_count
        };
    });
    callback(res);
}

exports.statsByType = async (client, callback) => {
    const response = await client.search({
        size: 0,
        index: indexName,
        body: {
            aggs: {
                types: {
                    terms: {
                        field: "type.keyword",
                        order: { _count: "desc" },
                        size: 5
                    },
                    aggs: {
                        sous_types: {
                            terms: {
                                field: "sous_type.keyword",
                                order: { _count: "desc" },
                                size: 5
                            }
                        }
                    }
                }
            }
        }
    });

    const res = response.body.aggregations.types.buckets.map(element => {
        return {
            "types": element.key,
            "count": element.doc_count,
            "sous_types": element.sous_types.buckets.map(sous_type => {
                return {
                    "sous_type": sous_type.key,
                    "count": sous_type.doc_count
                }
            })
        };
    });

    callback(res);
}

exports.statsByMonth = async (client, callback) => {
    const response = await client.search({
        size: 0,
        index: indexName,
        body: {
            aggs: {
                datesdeclaration: {
                    terms: {
                        field: "date_declaration.keyword",
                        order: { _count: "desc" },
                        size: 10
                    }
                }
            }
        }
    });
    const res = response.body.aggregations.datesdeclaration.buckets.map(element => {
        return {
            "month": element.key,
            "count": element.doc_count
        };
    });
    callback(res);
}

exports.statsPropreteByArrondissement = async (client, callback) => {
    const response = await client.search({
        size: 0,
        index: indexName,
        body: {
            query: {
                bool: {
                    must: {
                        match: {
                            type: "Propreté"
                        }
                    }
                }
            },
            aggs: {
                arrondissements: {
                    terms: {
                        field: "arrondissement.keyword",
                        order: { _count: "desc" },
                        size: 3
                    }
                }
            }
        }
    });
    const res = response.body.aggregations.arrondissements.buckets.map(element => {
        return {
            "arrondissement": element.key,
            "count": element.doc_count
        };
    });
    callback(res);
}
