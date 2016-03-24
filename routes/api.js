var express = require('express');
var router = express.Router();
var md = require('../market-data/market-data'); /* marketData module */

/* GET quotes listing. */
router.get('/quotes/', function(req, res, next) {
  res.send('respond with a resource');
});

/* GET specific quotes */
router.get('/quotes/:symbols', function(req, res, next) {
  var symbols = req.params.symbols.split(',');
  var opt = {
    refresh: true, /* Refresh cache */
  };

  md.getCompositeQuotes(symbols, opt, function(result) {
    res.json(result);
  });
});

module.exports = router;
