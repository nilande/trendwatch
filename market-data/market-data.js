var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('market-data/market-data.db');
var dataApis = require('./data-apis');

/*****************************************************************************
 * Initialization
 *****************************************************************************/

console.log('Opening database');
db.serialize(function() {
  // DB Settings to speed up writes, especially on NFS
  db.run("PRAGMA journal_mode = MEMORY");

  // Create tables
  db.run("CREATE TABLE IF NOT EXISTS quotes (symbol TEXT, date TEXT, close REAL)");

  // Create index
  db.run("CREATE UNIQUE INDEX IF NOT EXISTS ux_quotes_symbol_date ON quotes (symbol ASC, date ASC)")
});

process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);

/*****************************************************************************
 * Public function declarations
 *****************************************************************************/

/*
 * getCompositeQuotes retrieves quotes for composite symbols
 * compositeSymbols - array of composite symbols to retrieve, separated by colons
 * opt(.refresh) - refresh cache if this is defined
 * next - function to pass results to 
 */
function getCompositeQuotes(compositeSymbols, opt, next) {
  /* Create an array of raw symbols to retieve */
  var symbols = new Array();
  for (compositeSymbol of compositeSymbols) {
    for (symbol of compositeSymbol.split(':')) { 
        if(symbols.indexOf(symbol) < 0) symbols.push(symbol);
    }
  }

  /* Fetch quotes for all symbols */
  getQuotes(symbols, opt, function(result) {
    var combinedResult = new Object();

    /* Build response by looping through composite symbols */
    for (compositeSymbol of compositeSymbols) {
      var symbols = compositeSymbol.split(':');

      /* Build 2-dimensional array of symbols to combine */
      var tempRes = [];
      for (symbol of symbols) {
        tempRes.push(result[symbol]);
      }

      /* Reduce to a 1-dimensional array */
      combinedResult = tempRes.reduce(multiplyResults);
    }

	/* Return format dependent on settings */
	next(combinedResult);
  });
}

/*
 * getQuotes refreshes quote data into cache and returns quote history
 * symbols - array of symbols to retrieve
 * opt(.refresh) - refresh cache if this is defined
 * next - function to pass results to 
 */
function getQuotes(symbols, opt, next) {
  if (opt.refresh) { /* Refresh quotes before proceeding */    
    refreshQuotes(symbols, function() {
      getCachedQuotes(symbols, next);
    });
  } else { /* Rely on quotes from cache */
    getCachedQuotes(symbols, next);        
  }
}

// Exports
module.exports.getCompositeQuotes = getCompositeQuotes;
module.exports.getQuotes = getQuotes;

/*****************************************************************************
 * Private functions
 *****************************************************************************/

function cleanup() {
  console.log('Closing database');
  db.close();
  process.exit(0);
}

/*
 * Multiply results (used e.g. for currency conversion)
 */
function multiplyResults(s1, s2) {
    if (s1.length == 0) return s2; /* s1 empty indicates nothing to reduce */

    /* Create list of dates in s1 and filter out all dates not in s2 */
    var dates = []; for (var k in s1) dates.push(k); 
    dates = dates.filter(function(date) {
      return s2.hasOwnProperty(date);
    });

    var result = {}; /* Array to hold conversion result */

    /* Loop through the series and multiply */
    for (date of dates) {
      result[date] = s1[date] * s2[date];
    }

    return result;
}

/*
 * getCachedQuotes fetches cached market data from the database
 * symbols - array of market symbols
 * next - function to call with the array of results
 */
function getCachedQuotes(symbols, next) {
  var result = new Object();

  db.serialize(function() {
    var temp = new Array();
    symbols.forEach(function(symbol) { temp.push('?') });
    db.all("SELECT symbol, date, close FROM quotes WHERE symbol IN (" + 
            temp.join() + ") ORDER BY symbol ASC, date ASC", symbols, function(err, rows) {
      rows.forEach(function(row) {
        /* If needed, create new object for this symbol */
        if (!result[row.symbol]) result[row.symbol] = new Object();

        /* Add row data to object */
        result[row.symbol][row.date] = row.close;
      });
      next(result);
    });
  });
}

/*
 * getLastDates gets the last available dates of each symbol from the cache
 * symbols - array of market symbols
 * next - function to call with the array of results
 */
function getLastDates(symbols, next) {
  db.serialize(function() {
    var temp = new Array();
    symbols.forEach(function(symbol) { temp.push('?') });
    db.all("SELECT symbol, MAX(date) AS lastdate FROM quotes WHERE symbol IN (" + 
            temp.join() + ") GROUP BY symbol", symbols, function(err, rows) {
      next(rows);
    });
  });
}

/*
 * refreshQuotes downloads new quote information to the database
 * symbols - array for symbols to refresh
 * next - function to call back when complete
 */
function refreshQuotes(symbols, next) {
  var query = new Object();

  /* Initiate symbols array */
  symbols.forEach(function(symbol) {
    query[symbol] = "1980-01-01";
  });

  /* Check cache for latest date with the current symbol */
  getLastDates(symbols, function(lastdates) {
    /* Update symnbols array with dates from existing cache */        
    lastdates.forEach(function(symbol) {
      query[symbol.symbol] = symbol.lastdate;
    });

    /* Refresh quotes */
    dataApis.getQuotes(query, function(results) {
      storeQuotes(results, next);
    });
  });
}

/*
 * storeQuotes stores an array of quotes in the cache database
 * symbol - Symbol name, e.g. "DJIA"
 * quotes - Array for quotes having "date" and "close" properties
 */
function storeQuotes(quotes, next) {
  db.serialize(function() {
    /* Prepare statement */
    var stmt = db.prepare("INSERT OR REPLACE INTO quotes VALUES (?, ?, ?)");

    /* Begin transaction */
    db.run("BEGIN TRANSACTION");

    for (var symbol in quotes) {
      /* Save some lookups for big data loads */
      var tempQuotes = quotes[symbol];

      /* Insert all rows */
      for (date in tempQuotes) {
        if (tempQuotes[date] > 0) /* Skip values that are blank or zero */
          stmt.run(symbol, date, tempQuotes[date]);
      }
    }

    /* End transaction */
    db.run("END TRANSACTION", next);

    /* Finalize */
    stmt.finalize();
  });
}
