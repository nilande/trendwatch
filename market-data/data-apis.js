const http = require('http');
const https = require('https');

/*****************************************************************************
 * Settings
 *****************************************************************************/

/* Column names in which to expect closing values for specific symbols */
const closeCols = {
    'LBMA': 'USD (PM)'
};

/*****************************************************************************
 * Public function declarations
 *****************************************************************************/

/*
 * getQuotes retrieves an array of quotes for a specific market symbol
 * query - Object containing symbol keys and start date values
 * callback - function that the result should be handed to
 */
function getQuotes(query, callback) {
    var to = new Date();
    var pending = 0;
    var results = new Object();

    /* Create collection function that is called irrespective of data source */
    var collect = function(symbol, result) {
        pending--; /* One pending result less */
        results[symbol] = result;
        /* Call callback if no more pending results */
        if (pending == 0) callback(results);
    };

    /* Loop through symbols */
    for (var symbol in query) {
        pending++; /* Keep track of pending results */
        getQuote(symbol, query[symbol], collect);
    }
}

// Exports
module.exports.getQuotes = getQuotes;

/*****************************************************************************
 * Private function declarations
 *****************************************************************************/

/*
 * getQuote retrieves quote information from YahooFinance or Quandl
 * symbol - the symbol to retireve
 * from - start of date range
 * next - function to pass results to
 */
function getQuote(symbol, from, next) {
    var result;

    /* Define callback function to process HTTP(s) results */
    var callback = function(res) {
        /* On error, return empty result */
        if (res.statusCode != 200) {
            result = [];
            next(symbol, {});
            return;
        }

        res.setEncoding('utf8');

        /* Add chunks of data to result */
        var body = '';
            res.on('data', function (chunk) {
            body += chunk;
        });

        /* At completion, process results and call back to next function */
        res.on('end', function() {
            result = processQuotes(symbol, body);
            next(symbol, result);
        });
    }

    switch(symbol) {
        case 'LBMA':
            /* Some symbols are collected from Quandl instead of Yahoo Finance */
            /* Define URL, submit HTTPS request and process results */
            /* Note: If you have an API key, set it in the QUANDL_API_KEY environment var */
            var url = 'https://www.quandl.com/api/v3/datasets/LBMA/GOLD.csv?'
            + (process.env.QUANDL_API_KEY ? ('api_key=' + process.env.QUANDL_API_KEY + '&') : '')
            + 'start_date=' + from;
            https.request(url, callback).end();
            break;

        default:
            /* Default is to collect from Yahoo Finance */
            /* Define URL, submit HTTP request and process results */
            var d1 = new Date(from), d2 = new Date();
            var url = "http://ichart.finance.yahoo.com/table.csv?s=" + symbol
            + "&a=" + d1.getMonth() + "&b=" + d1.getDate() + "&c=" + d1.getFullYear()
            + "&d=" + d2.getMonth() + "&e=" + d2.getDate() + "&f=" + d2.getFullYear() 
            + "&g=&q=q&y=0&z=" + symbol + "&x=.csv"
            http.request(url, callback).end();
            break;
    }
}

/*
 * processQuotes converts the CSV results to an array
 * symbol - the symbol to retireve
 * body - csv text to process 
 */
function processQuotes(symbol, body) {
    /* Split into array of lines */
    var lines = body.split('\n');

    /* Split headings and find columns indeces of "Date" and "Close" columns */
    var headings = lines.splice(0,1)[0].split(',');
    var result = new Object();
    var dateCol = headings.indexOf("Date");
    
    /* Lookup column in which to find closing values from the settings map */
    var closeCol = headings.indexOf(closeCols[symbol] || 'Close');

    /* Loop through lines and store date and close values to result array */
    lines.forEach(function(line) {
        if (line != '') {
           var cells = line.split(',');
           result[cells[dateCol]] = cells[closeCol];
        }
    });

    return result;
}

