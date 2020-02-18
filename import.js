const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');
const MAX_CHUNK_SIZE = 5000;

async function run () {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    
    // Création de l'indice
    await client.indices.create({ index: indexName });

    await client.indices.putMapping({
        index: indexName,
        body: {
            properties: {
                location: {
                    type: "geo_point"
                }
            }
        }
    });
    console.log(`Index ${indexName} cree`)


    let anomalies = [];

    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            anomalies.push({
                "@timestamp": data.DATEDECL,
                object_id: data.OBJECTID,
                annee_declaration: data["ANNEE DECLARATION"],
                mois_declaration: data["MOIS DECLARATION"],
                date_declaration: `${data["MOIS DECLARATION"]}/${data["ANNEE DECLARATION"]}`,
                type: data.TYPE,
                sous_type: data.SOUSTYPE,
                code_postal: data.CODE_POSTAL,
                ville: data.VILLE,
                arrondissement: data.ARRONDISSEMENT,
                prefixe: data.PREFIXE,
                intervenant: data.INTERVENANT,
                conseil_de_quartier: data["CONSEIL DE QUARTIER"],
                location: data.geo_point_2d
            });

            if (anomalies.length > MAX_CHUNK_SIZE) {
                client.bulk(createBulkInsertQuery(anomalies));
                anomalies = [];
            }

        })
        .on('end', () => {
            try {
                client.bulk(createBulkInsertQuery(anomalies));
            } catch (err) {
                console.trace(err);
            } finally {
                client.close();
            }
            console.log('Terminated!');
        })
        .on('error', (err) => {
            console.trace(err);
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(anomalies) {
    const body = anomalies.reduce((acc, anomaly) => {
      const { object_id, ...params } = anomaly;
      acc.push({
        index: { _index: indexName, _type: "_doc", _id: object_id }
      });
      acc.push(params);
      return acc;
    }, []);
  
    return { body };
}



run().catch(console.error);
