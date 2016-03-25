var marketData = require('./market-data');

marketData.getCompositeQuotes(['LBMA:SEK=X'], {refresh: false}, function(result) {
  console.log(result);
});


