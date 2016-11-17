var mysql = require('mysql');
var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectID;
var Promise = require('bluebird');

// var uri = 'mongodb://localhost:27017/vision2';
var uri = 'mongodb://heroku_9d3ppsr0:a706flgp82q7qenmd166qmvq8d@ds051655.mlab.com:51655/heroku_9d3ppsr0';

var connection = mysql.createConnection({
  host: '192.168.53.120',
  user: 'root',
  password: '.root4mysql',
  database: 'vts_stage'
});

connection.connect();

connection.query('SELECT BsnsInfoID, BsnsName, Adrs1, Adrs2, City, Stat, Ctry, Zip, Phone ' +
  'FROM BsnsInfo', function (err, rows, fields) {
    if (err) throw err;

    MongoClient
      .connect(uri)

      .then(function (db) {
        var clients = db.collection('clients');

        return Promise
          .each(rows, function (row, index) {
            var doc = {
              oldId: row.BsnsInfoID,
              name: row.BsnsName,
              address1: row.Adrs1,
              address2: row.Adrs2,
              city: row.City,
              state: row.Stat,
              country: row.Ctry,
              zip: row.Zip,
              phone: row.Phone
            };

            return clients.insertOne(doc).then(function (result) {
              console.log(index, result.result);
            });
          })

          .then(function (rows) {
            console.log('done');
            return;
          });

      }).catch(function (err) {
        throw err;
      });
  });