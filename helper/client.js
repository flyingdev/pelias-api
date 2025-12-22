require('dotenv').config();
const { Client: ESClient } = require('elasticsearch');
const { Client: OSClient } = require('@opensearch-project/opensearch');
const { get } = require('lodash');
const peliasConfig = require('pelias-config').generate();


function getDatabaseConfig() {
  const config = peliasConfig.get('dbclient') || peliasConfig.get('esclient');
  
  if (!config) {
    throw new Error('Database configuration missing in pelias.json');
  }

  const engine = config.engine || (process.env.PELIAS_OPENSEARCH === 'true' ? 'opensearch' : 'elasticsearch');

  return { ...config, engine };
}
/**
 * Factory for creating a search client.
 */
function createClient(peliasConfig) {
  const { engine, hosts } = getDatabaseConfig();
  const hostConfig = get(hosts, '[0]');
  if (!hostConfig) {
    throw new Error(
      '[api] No node URL found. Please configure dbclient.hosts in pelias.json.'
    );
  }
  const { protocol, host, port } = hostConfig;
  const node = `${protocol}://${host}:${port}`;

  if (engine === 'opensearch') {
    console.log(`[api] Using OpenSearch node: ${node}`);
    return new OSClient({ node });
  } else {
    console.log(`[api] Using Elasticsearch node: ${node}`);
    return new ESClient(peliasConfig.dbclient || {});
  }
}

function normalizeQuery(client, query) {
  const clone = { ...query };

  // If OpenSearch client, fix parameter casing
  if (client.constructor.name === 'Client' && client.transport?.connectionPool) {
    // @opensearch-project/opensearch client detected
    if ('requestCache' in clone) {
      clone.request_cache = clone.requestCache;
      delete clone.requestCache;
    }
  }
  return clone;
}

function compatSearch(client, query, callback) {
  const normalizedQuery = normalizeQuery(client, query);

  client.search(normalizedQuery, (err, res) => {
    if (err) {
      return callback(err);
    }

    // OpenSearch uses res.body, ES returns res directly
    const body = res?.body || res;

    callback(null, body);
  });
}


module.exports = { 
  createClient, 
  getDatabaseConfig,
  compatSearch
};