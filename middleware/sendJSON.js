const _ = require('lodash');
const os = require('@opensearch-project/opensearch');
const PeliasParameterError = require('../sanitizer/PeliasParameterError');
const PeliasTimeoutError = require('../sanitizer/PeliasTimeoutError');

function isParameterError(error) {
  return error instanceof PeliasParameterError;
}

function isTimeoutError(error) {
  return error instanceof PeliasTimeoutError ||
         error instanceof os.errors.TimeoutError;
}

function isOpenSearchError(error) {
  const knownErrors = [ os.errors.NoLivingConnectionsError,
                        os.errors.ConnectionError ];

  return knownErrors.some(function(esError) {
    return error instanceof esError;
  });
}

function sendJSONResponse(req, res, next) {

  // do nothing if no result data set
  const geocoding = _.get(res, 'body.geocoding');

  if (!_.isPlainObject(geocoding)) {
    return next();
  }

  const errors = geocoding.errors || [];

  const errorCodes = errors.map(function(error) {
    if (isParameterError(error)) {
      return 400;
    } else if (isTimeoutError(error) || isOpenSearchError(error)) {
      return 502;
    } else {
      return 500;
    }
  });

  const statusCode = Math.max(200, ...errorCodes);

  // respond
  return res.status(statusCode).json(res.body);
}

module.exports = sendJSONResponse;
