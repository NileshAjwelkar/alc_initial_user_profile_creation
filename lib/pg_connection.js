var PGConnection = function () {};

require('dotenv').config();
var pg = require('pg');
var Pool = require('pg-pool');

var configPG = {
    user: process.env.RDS_USER,
    database: process.env.RDS_DB_NAME,
    password: process.env.RDS_PASSWORD,
    host: process.env.RDS_HOST,
    port: process.env.RDS_PORT,
    ssl: true,
    min: parseInt(process.env.RDS_MIN_CLIENT),
    max: parseInt(process.env.RDS_MAX_CLIENT), // max number of clients in the pool
    idleTimeoutMillis: parseInt(process.env.RDS_TIMEOUT), // how long a client is allowed to remain idle before being closed
    Promise: require('promise-polyfill')
};

PGConnection.prototype.client = new Pool(configPG);

module.exports = new PGConnection();