var logger = require( 'pelias-logger' ).get( 'api' );

function service( searchClient, query, cb ){

  // opensearch command
  var cmd = {
    body: {
      docs: query
    }
  };

  // query opensearch
  const startTime = new Date();
  searchClient.mget( cmd, function( err, data ){
    if (data) {
      data.response_time = new Date() - startTime;
    }

    // handle opensearch errors
    if( err ){
      logger.error( `opensearch error ${err}`);
      return cb( err );
    }

    // map returned documents
    var docs = [];
    if( data && Array.isArray(data.docs) ){

      docs = data.docs.filter( function( doc ){

        // remove docs not actually found
        return doc.found;

      }).map( function( doc ){

        // map metadata in to _source so we
        // can serve it up to the consumer
        doc._source._id = doc._id;

        return doc._source;
      });
    }

    // fire callback
    return cb( null, docs, data );
  });

}

module.exports = service;
