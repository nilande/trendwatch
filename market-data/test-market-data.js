var marketData = require('./market-data');

marketData.getQuotes(['^OMXSPI', 'LBMA'], {refresh: true}, function(result) {
  console.log(result);
});


