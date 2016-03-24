var http = require('http');
const https = require('https');

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

        switch(symbol) {
            case 'LBMA':
                /* Some symbols are collected from Quandl instead of Yahoo Finance */
                getQuoteQuandl(symbol, query[symbol], collect);
                break;

            default:
                /* Default is to collect from Yahoo Finance */
                getQuoteYF(symbol, new Date(query[symbol]), new Date(), collect);
                break;
        }
    }
}

// Exports
module.exports.getQuotes = getQuotes;

/*****************************************************************************
 * Private function declarations
 *****************************************************************************/

/*
 * getQuoteQuandl retrieves quote information from Quandl
 * symbol - the symbol to retireve. CURRENTLY ONLY HANDLES LBMA
 * from - start of date range
 * callback - function to pass results to
 */
function getQuoteQuandl(symbol, from, callback) {
    var result;

    /* HTTP request details for Yahoo Finance */
    var url = "https://www.quandl.com/api/v3/datasets/LBMA/GOLD.csv?start_date=" + from

    /* Submit HTTP request and process results */
    https.request(url, function(res) {
        /* On error, return empty result */
        if (res.statusCode != 200) {
            result = [];
            callback(symbol, []);            
            return;
        }

        res.setEncoding('utf8');

        /* Add chunks of data to result */
        var body = '';
            res.on('data', function (chunk) {
            body += chunk;
        });

        /* At completion, process results and call back */
        res.on('end', function() {
            result = processQuotesQuandl(symbol, body);
            callback(symbol, result);
        });
    }).end();
}

/*
 * getQuoteYF retrieves quote information from YahooFinance
 * symbol - the symbol to retireve
 * from - start of date range
 * to - end of date range
 * callback - function to pass results to
 */
function getQuoteYF(symbol, from, to, callback) {
    var result;

    /* HTTP request details for Yahoo Finance */
    var options = {
        host: "ichart.finance.yahoo.com",
        path: "/table.csv?s=" + symbol
        + "&a=" + from.getMonth() + "&b=" + from.getDate() + "&c=" + from.getFullYear()
        + "&d=" + to.getMonth() + "&e=" + to.getDate() + "&f=" + to.getFullYear() 
        + "&g=&q=q&y=0&z=" + symbol + "&x=.csv"
    };        

    /* Submit HTTP request and process results */
    http.request(options, function(res) {
        /* On error, return empty result */
        if (res.statusCode != 200) {
            result = [];
            callback(symbol, []);            
            return;
        }

        res.setEncoding('utf8');

        /* Add chunks of data to result */
        var body = '';
            res.on('data', function (chunk) {
            body += chunk;
        });

        /* At completion, process results and call back */
        res.on('end', function() {
            result = processQuotesYF(body);
            callback(symbol, result);
        });
    }).end();
}

/*
 * processQuotes converts the CSV results to an array
 * symbol - the symbol to retireve. CURRENTLY ONLY HANDLES LBMA
 * body - csv text to process 
 */
function processQuotesQuandl(symbol, body) {
    /* Split into array of lines */
    var lines = body.split('\n');

    /* Split headings and find columns indeces of "Date" and "Close" columns */
    var headings = lines.splice(0,1)[0].split(',');
    var result = new Array();
    var dateCol = headings.indexOf("Date");
    var closeCol = headings.indexOf("USD (PM)");

    /* Loop through lines and store date and close values to result array */
    lines.forEach(function(line) {
        if (line != '') {
           var cells = line.split(',');
           result.push({ date: cells[dateCol], close: cells[closeCol] });
        }
    });

    /* Reorder by date */
    result.sort(function(a,b) { return a.date.localeCompare(b.date) });

    return result;
}
/*
 * processQuotes converts the CSV results to an array
 * body - csv text to process 
 */
function processQuotesYF(body) {
    /* Split into array of lines */
    var lines = body.split('\n');

    /* Split headings and find columns indeces of "Date" and "Close" columns */
    var headings = lines.splice(0,1)[0].split(',');
    var result = new Array();
    var dateCol = headings.indexOf("Date");
    var closeCol = headings.indexOf("Close");

    /* Loop through lines and store date and close values to result array */
    lines.forEach(function(line) {
        if (line != '') {
           var cells = line.split(',');
           result.push({ date: cells[dateCol], close: cells[closeCol] });
        }
    });

    /* Reorder by date */
    result.sort(function(a,b) { return a.date.localeCompare(b.date) });

    return result;
}

