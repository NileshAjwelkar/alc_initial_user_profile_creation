require('dotenv').config();

var Athena = require("../lib/athena");
var async = require("async");
var https = require('https');
var AWS = require('aws-sdk');
var url = require('url');
var xls = require('node-xlsx');
var csv = require('csv-parse');
var fs = require('fs');
var s3 = require('s3');
var awsCli = require('aws-cli-js');
var https = require('https');

var JDBC = require("jdbc");
var jinst = require("jdbc/lib/jinst");
var moment = require("moment");

var AWSOptions = awsCli.Options;
var Aws = awsCli.Aws;

var aws_options = new AWSOptions(
        process.env.S3_USER_KEY,
        process.env.S3_USER_SECRET,
        null
);
var aws = new Aws(aws_options);
var athena = new Athena(process.env.ALC_ONBOARDING_ONLINE_DATA_PROCESSING);

AWS.config.update({
    accessKeyId: process.env.S3_USER_KEY,
    secretAccessKey: process.env.S3_USER_SECRET,
    region: process.env.REGION
});

var query = "SELECT distinct t1.third_party_id as guid, 'email' as hashed_value_type, t2.hash_type as hash_type, concat('email-',t2.hash_type,'-',t2.email_hash) as hashed_value \
FROM pixel_fires_datalake_primary t1 \
CROSS JOIN unnest ( \
array['uc_sha1', 'lc_sha1', 'lc_md5', 'uc_sha256', 'lc_sha256' ], \
array[hashed_email_uc_sha1, hashed_email_lc_sha1, hashed_email_lc_md5, hashed_email_uc_sha256, hashed_email_lc_sha256] \
) t2 (hash_type, email_hash) \
WHERE year = 2017 \
AND month = 07 \
AND day = 15 \
AND date_key = 20170715 \
AND email_hash <> '' \
AND email_hash is not null \
AND email_hash <> 'NULL' \
AND third_party_id is not null \
AND third_party_id <> 'NULL' \
AND REGEXP_LIKE(email_hash, '^[a-fA-F0-9]+') LIMIT 1000";


var dynamoDbTable = process.env.DYNAMODB_USER_PROFILE_TABLE;
var dynamoDbData = {};
dynamoDbData["RequestItems"] = {};
dynamoDbData["RequestItems"][dynamoDbTable] = [];
var dynamodb = new AWS.DynamoDB();

var s3 = new AWS.S3();
var expiryInDays = 45;

var run_athena_query = false;

/**************************************** ****************************************/

var gets3FileName = function (callback) {
    startDate = new Date();
    var s3GetQuery = "s3 ls" + " " + process.env.ALC_ONBOARDING_ONLINE_DATA_PROCESSING;
    console.log("s3GetQuery :"+s3GetQuery);
    var s3FileNameObj = {};
    aws.command(s3GetQuery).then(function (data) {
        if (data.error) {
            callback ( data.error, null);
        } else {
            var tempArr = data.raw.split("\n");
            for (var i = 0; i < tempArr.length; i++) {
                if (tempArr[i].match(/.csv/g)) {
                    var tempArrRow = tempArr[i].split(" ");
                    if (tempArrRow[tempArrRow.length - 1].match(/.csv.metadata/g)) {
                        s3FileNameObj['meta'] = tempArrRow[tempArrRow.length - 1];
                    } else {
                        s3FileNameObj['main'] = tempArrRow[tempArrRow.length - 1];
                    }
                }
            }
            callback(null, s3FileNameObj);
        }
    });
};

var moveS3Files = function(filename, bucket, callback) {
    
    var s3mvQuery = "s3 mv" + " " + process.env.ALC_ONBOARDING_ONLINE_DATA_PROCESSING + filename + " " + bucket;
    console.log(s3mvQuery);
    aws.command(s3mvQuery).then(function(data) {
        if (data.error) {
            callback(data.error, null);
        } else {
            callback(null, data);
        }
    });

};

var getDataFromCSV = function(s3fileName, bucket, processing_folder, callback){

    var file = fs.createWriteStream("./file.csv");
    var s3Params = {
        Bucket: bucket,
        Key: processing_folder + s3fileName
    };

    s3.getObject(s3Params).createReadStream().on('error', function(err,data){
        console.log(err);
    }).pipe(file);

    file.on('finish', function(err, data){
        fs.createReadStream("./file.csv").pipe(csv({ delimiter: ',' }, function(err, data) {
            if (err){
                console.log("Error : " + err);
                callback(err, null);
            }
            if (data){
                // console.log("data : " + data);
                callback(null, data);
            }
        }));
    });

};

var formatResponse = function(parsedResponse, callback) {  
    parsedResponse.shift();
    var batchedDataArray = [];

    while(parsedResponse.length) {
        batchData = parsedResponse.splice(0,25);
        var dynamoDbData = {};
        dynamoDbData["RequestItems"] = {};
        dynamoDbData["RequestItems"][dynamoDbTable] = [];

        for(var i =0; i< batchData.length; i++){
            rowData = batchData[i];
            online_id = rowData[0];
            hashed_value_type = rowData[1];
            hash_type = rowData[2];
            hashed_value = rowData[3];

            var todayDate = new Date();
            
            var expiryTimestamp = new Date();
            arrayData = {"PutRequest": {"Item": {}}};
            arrayData["PutRequest"]["Item"] = {"online_id": {}, "hash_type": {}, "hashed_value": {}, "hashed_value_type": {},
                            "create_timestamp": {}, "update_timestamp": {}, "ttl": {}, "total_counts": {}};//, "first_party_id": {}};
                
            arrayData["PutRequest"]["Item"]["online_id"]["S"] = online_id;    
            arrayData["PutRequest"]["Item"]["hash_type"]["S"] = hash_type;
            arrayData["PutRequest"]["Item"]["hashed_value_type"]["S"] = hashed_value_type;
            arrayData["PutRequest"]["Item"]["hashed_value"]["S"] = hashed_value;
            // arrayData["PutRequest"]["Item"]["first_party_id"]["S"] = "" 

            arrayData["PutRequest"]["Item"]["create_timestamp"]["N"] = String(Math.floor(todayDate.getTime()/1000));
            arrayData["PutRequest"]["Item"]["update_timestamp"]["N"] = String(Math.floor(todayDate.getTime()/1000));

            expiryTimestamp.setDate(todayDate.getDate() + expiryInDays);
            arrayData["PutRequest"]["Item"]["ttl"]["N"] = String(Math.floor(expiryTimestamp.getTime()/1000));
            arrayData["PutRequest"]["Item"]["total_counts"]["N"] = "1";

            dynamoDbData["RequestItems"][dynamoDbTable].push(arrayData);                
        }
        batchedDataArray.push(dynamoDbData);
    }
     callback(null, batchedDataArray);
};

var addToDynamoDB = function(batchedDataArray, callback){
    console.log("Total batches ==> " + batchedDataArray.length);
    for(var i=0; i<batchedDataArray.length; i++) {
        dynamodbData = batchedDataArray[i];
        console.log(" Batch " + (i + 1) + " of " + batchedDataArray.length + " ==> " + JSON.stringify(dynamodbData) + "\n\n");
        console.log(" Batch Length ==> " + JSON.stringify(dynamodbData["RequestItems"][dynamoDbTable].length));
        dynamodb.batchWriteItem(dynamodbData, function (err, data) {
            if (err) {
                console.log('err');
                console.log(err, err.stack);
                callback(err, null);
            } else {
                console.log('ok');
                console.log(JSON.stringify(data));
                callback(null, data);
            } 
        });
    }
};

var benchmarkFunctions = function(startTime, endTime, processName){
    millisecs = endTime - startTime;
    var minutes = Math.floor(millisecs / 60000);
    var seconds = ((millisecs % 60000) / 1000).toFixed(0);
    durationInSecs = (seconds == 60 ? (minutes+1) + ":00" : minutes + ":" + (seconds < 10 ? "0" : "") + seconds);
    console.log("The process : " + processName + " started at " + startTime + " and ended at " + endTime + " and took " + durationInSecs)
}; 

var convertTOBinary = function(strObj){
    return new Buffer(strObj);
}

/**************************************** ****************************************/ 

async.waterfall([
    function executeAthenaQuery(callback){
       if(run_athena_query){
            athena.executeQuery(query, function(err, data) {
                console.log(" --------- executeAthenaQuery : Done --------- ");
                console.log(" data :" + data);
                callback(null, data);
            });
       }
       else{
           console.log(" --------- executeAthenaQuery : RUN_ATHENA_QUERY_STATUS : " + run_athena_query);
           callback(null, "");
       }
    },
    function fetch3FileName(data, callback){
        
        if(run_athena_query){
            gets3FileName(function (err, resultsS3){
                console.log(" --------- fetch3FileName : Done --------- ");
                callback(null, resultsS3);
            });
        }
        else{
            console.log(" --------- fetch3FileName : RUN_ATHENA_QUERY_STATUS : " + run_athena_query);
            callback(null, "");
        }
    },
    function copyS3MetaFiles(resultsS3, callback){
        
        if(run_athena_query){
            console.log(" resultsS3 Meta :" + JSON.stringify(resultsS3));
            moveS3Files(resultsS3.meta, process.env.ALC_ONBOARDING_ONLINE_DATA_META, function (err, result){
                console.log(" --------- copyS3Files : Done --------- ");
            });
            callback(null, resultsS3);
        }
        else {
            console.log(" --------- copyS3Files : RUN_ATHENA_QUERY_STATUS : " + run_athena_query);
            callback(null, resultsS3);
        }
        
    },
    function parseResponseCsv(resultsS3, callback){
        if(run_athena_query){
            var s3FileName = resultsS3.main;
            var s3Bucket =  process.env.ALC_ONBOARDING_BUCKET;
            var processing_folder = process.env.ALC_ONBOARDING_PROCESSING_FOLDER
        }
        else {
            console.log(" --------- parseResponseCsv : RUN_ATHENA_QUERY_STATUS : " + run_athena_query);

            console.log("process.argv ==> " + JSON.stringify(process.argv));

            var s3FileName = process.argv[2] || "updated_query.csv";
            var s3Bucket =  process.env.ALC_ONBOARDING_BUCKET;
            var processing_folder = "online-data/testS3Read/";
        }
        
        getDataFromCSV(s3FileName, s3Bucket, processing_folder,function(err, parsedData){
            console.log(" --------- getDataFromCSV : Done --------- ");
            callback(null, parsedData, resultsS3);
         });
         
    },
    function formatCsvResponse(resultSet, resultsS3, callback){
        formatResponse(resultSet, function(err, formattedData) {
            console.log(" --------- formatResponse : Done --------- ");
            callback(null, formattedData, resultsS3);
        });
    }
    ,
    function insertInDynamoDb(formattedDynamoData, resultsS3, callback){
        addToDynamoDB(formattedDynamoData, function(err, resp){
            console.log(" --------- insertInDynamoDb : Done --------- ");
            callback(null, resultsS3)
        });
    }
    ,
    function copyS3MainFiles(resultsS3, callback){
        console.log(" resultsS3 Main :" + JSON.stringify(resultsS3));
    
        if(run_athena_query){
            console.log(" resultsS3 :" + JSON.stringify(resultsS3));
            moveS3Files(resultsS3.main, process.env.ALC_ONBOARDING_ONLINE_DATA_META, function (err, result){
                console.log(" --------- copyS3Files : Done --------- ");
            });
            callback(null, resultsS3);        
        }
        else{
            console.log(" --------- copyS3Files : RUN_ATHENA_QUERY_STATUS : " + run_athena_query);
            callback(null, 'done');   
        }
    }
],
    function(err, data) {
        var message = '';
        if (err) {
            console.log("Error: ", err);
            process.exit();
        } else {
            console.log('Success!');
            process.exit();
        }
    }
);