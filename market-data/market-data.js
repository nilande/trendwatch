var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('market-data/market-data.db');
var di = require('./data-apis');

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
 * compositeSymbols - array of composite symbols to retrieve
 * refresh - refresh cache if this is defined
 * callback - function to pass results to 
 */
function getCompositeQuotes(compositeSymbols, opt, callback) {
  /* Create a list of raw symbols to retieve */
  var symbols = new Array();
  compositeSymbols.forEach(function(compositeSymbol) {
      compositeSymbol.split(':').forEach(function(symbol) { 
          if(symbols.indexOf(symbol) < 0) symbols.push(symbol);
      });
  });

  getQuotes(symbols, opt, function(result) {
    var combinedResult = new Object();

    /* Combine results into original composites */
    compositeSymbols.forEach(function(compositeSymbol) {
      var symbols = compositeSymbol.split(':');
      switch (symbols.length) {
        case 1:
          /* Composite symbol is a simple symbol, just return the result */
          combinedResult[symbols[0]] = result[symbols[0]];
          break;

        case 2:
          /* Composite symbol requires combining before returning */
          combinedResult[compositeSymbol] = combineResults(result[symbols[0]],result[symbols[1]]);
          break;

        default:
      }
    });

	/* Return format dependent on settings */
	callback(combinedResult);
  });
}

/*
 * getQuotes refreshes quote data into cache and returns quote history
 * symbols - array of symbols to retrieve
 * refresh - refresh cache if this is defined
 * callback - function to pass results to 
 */
function getQuotes(symbols, opt, callback) {
    if (opt.refresh) { /* Refresh quotes before proceeding */    
        refreshQuotes(symbols, function() {
            getCachedQuotes(symbols, callback);
        });
    } else { /* Rely on quotes from cache */
        getCachedQuotes(symbols, callback);        
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
 * Combine results (default action is multiplying, e.g. for currency convertion
 */
function combineResults(s1, s2) {
    var i1 = 0, i2 = 0; 
    var result = new Array();    /* Array to hold conversion result */

    /* Loop through the series and multiply */
    while (i1 < s1.length && i2 < s2.length) {
        var cmp = s1[i1].date.localeCompare(s2[i2].date);

        if (cmp < 0) { // s1's date is earlier than s2
            i1++;
        } else if (cmp > 0) { // s2's date is earlier than s1
            i2++;
        } else { // Matching dates, combine!
            result.push({
                date: s1[i1].date,
                close: s1[i1].close * s2[i2].close
            });
            i1++;
            i2++;
        }
    }

    return result;
}

/*
 * getCachedQuotes fetches cached market data from the database
 * symbols - array of market symbols
 * callback - function to call with the array of results
 */
function getCachedQuotes(symbols, callback) {
    var result = new Object();

    db.serialize(function() {
        var temp = new Array();
        symbols.forEach(function(symbol) { temp.push('?') });
        db.all("SELECT symbol, date, close FROM quotes WHERE symbol IN (" + 
                temp.join() + ") ORDER BY symbol ASC, date ASC", symbols, function(err, rows) {
            rows.forEach(function(row) {
                if (!result[row.symbol]) result[row.symbol] = new Array();
                result[row.symbol].push({
                    date: row.date,
                    close: row.close
                });
            });
            callback(result);
        });
    });
}

/*
 * getLastDates gets the last available dates of each symbol from the cache
 * symbols - array of market symbols
 * callback - function to call with the array of results
 */
function getLastDates(symbols, callback) {
    db.serialize(function() {
        var temp = new Array();
        symbols.forEach(function(symbol) { temp.push('?') });
        db.all("SELECT symbol, MAX(date) AS lastdate FROM quotes WHERE symbol IN (" + 
                temp.join() + ") GROUP BY symbol", symbols, function(err, rows) {
            callback(rows);
        });
    });
}

/*
 * refreshQuotes downloads new quote information to the database
 * symbols - array for symbols to refresh
 * callback - function to call back when complete
 */
function refreshQuotes(symbols, callback) {
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
        
        di.getQuotes(query, function(results) {
            storeQuotes(results, callback);
        });
    });
}

/*
 * storeQuotes stores an array of quotes in the cache database
 * symbol - Symbol name, e.g. "DJIA"
 * quotes - Array for quotes having "date" and "close" properties
 */
function storeQuotes(quotes, callback) {
    db.serialize(function() {
        /* Prepare statement */
        var stmt = db.prepare("INSERT OR REPLACE INTO quotes VALUES (?, ?, ?)");

        /* Begin transaction */
        db.run("BEGIN TRANSACTION");

        for (var symbol in quotes) {
            /* Insert all rows */
            for (var i = 0; i < quotes[symbol].length; i++) {
		if (quotes[symbol][i].close > 0) /* Skip values that are blank or zero */
                  stmt.run(symbol, quotes[symbol][i].date, quotes[symbol][i].close);
            }
        }

        /* End transaction */
        db.run("END TRANSACTION", callback);

        /* Finalize */
        stmt.finalize();
    });
}
