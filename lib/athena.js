require('dotenv').config({path: __dirname + '/./../.env'})
var async = require("async");
var JDBC = require("jdbc");
var jinst = require("jdbc/lib/jinst");

var prototype = Athena.prototype;

function Athena(processingBucket) { 
  this.jInstance();
  this.jdbcInstance = new JDBC(this.jdbcConfig(processingBucket));
};

prototype.jInstance = function() {
  if (!jinst.isJvmCreated()) {
      jinst.addOption("-Xrs");
      jinst.setupClasspath([process.env.ATHENA_DRIVER_PATH]);
  }  
};

prototype.jdbcConfig = function(processingBucket) {
    console.log('processingBucket: ', processingBucket);
  return {
    url: process.env.ATHENA_URL,
    drivername: "com.amazonaws.athena.jdbc.AthenaDriver",
    properties: {
        user: process.env.S3_USER_KEY,
        password: process.env.S3_USER_SECRET,
        s3_staging_dir: processingBucket
    }
  }
};

prototype.executeQuery = function(query, callback) {
    var queryStr = query.toString();
    var jdbc = this.jdbcInstance;
    console.log(queryStr);
    jdbc.initialize(function(err) {
        if (err) {
            console.log("ERROR: ", err);
        }
    });
    jdbc.reserve(function(err, connectionObject) {
        if (connectionObject) {
            console.log("Using connection: " + connectionObject.uuid);
            var connection = connectionObject.conn;

            connection.createStatement(function(err, statement) {
                if (err) {
                    callback(err);
                } else {
                    statement.executeQuery(queryStr, function(err, resultset) {
                        if (err) {
                            callback(err, null);
                        } else {
                            callback(null, resultset);
                        }
                    });
                }
            });
        }
    });
};


module.exports = Athena;