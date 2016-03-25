var dataApis = require('./data-apis');

dataApis.getQuotes({'^OMXSPI': '2016-02-01',
                    'LBMA': '2016-03-01'}, function(result) {
  console.log(result);
});


