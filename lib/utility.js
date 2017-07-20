var Utility = function () {};

var path = require('path');
var PGConnection = require( path.resolve( __dirname, "./pg_connection.js" ) );

Utility.prototype.formatHeader = function(headerName) {
    return headerName.replace(/[^\w\s_]/gi, ' ').replace(/\s+/g, "_").replace(/__+/g, '_').replace(/^__*/, '').replace(/__*$/, '').toLowerCase();
}

Utility.prototype.executeQuery = function(sqlQuery, callback) {
    PGConnection.client.connect(function(err, client, done) {
        if (err) {
            return console.error('Error fetching client from pool', err);
        }
        client.query(sqlQuery, function(err, result) {
            done();
            if (err) {
                console.log(err);
                callback(err, 'error');
            } else {
                callback(null, result);
            }
        });
    });
};


Utility.prototype.replaceQuotes = function(data) {
    var headerArray = [];
    for (var i = 0; i < data.length; i++) {
            headerArray.push({ 'header': data[i].replace(/['"]+/g, '') });
    }
    return headerArray;
}


Utility.prototype.buildBatchInsertQuery = function(tableName, dataValues){
    var insertFields = dataValues[0]["data"];
    var queryInsertValues = "INSERT INTO " + tableName + " (" + Object.keys(insertFields).join(',') + ") VALUES ";
    
    for (var i = 0; i < dataValues.length; i++){
        insertData = dataValues[i].data;
        queryInsertValues += "(";
        var counter = 0
        for (var property in insertData)
        {
            counter += 1
            queryInsertValues += JSON.stringify(insertData[property]).replace(/"+/g,"'");
            if (counter < Object.keys(insertData).length){
                queryInsertValues += ","
            }
        }
        queryInsertValues += "),"
    }
    queryInsertValues = queryInsertValues.replace(/,*$/, ';');
    return queryInsertValues;
}

module.exports = new Utility();


