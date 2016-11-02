var mysql = require('mysql');
var MongoClient = require('mongodb').MongoClient;
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

    MongoClient.connect(uri, function (err, db) {
      if (err) throw err;

      var clients = db.collection('clients');

      function processRow(rows, i) {
        if (i == rows.length) {
          console.log('done');
          return;
        }

        var row = rows[i];

        var doc = {
          OldId: row.BsnsInfoID,
          Name: row.BsnsName,
          Address1: row.Adrs1,
          Address2: row.Adrs2,
          City: row.City,
          State: row.Stat,
          Country: row.Ctry,
          Zip: row.Zip,
          Phone: row.Phone
        };

        clients.insertOne(doc, function (err, result) {
          if (err) throw err;

          console.log(i, result.result);

          processRow(rows, i + 1);
        });
      }

      processRow(rows, 0);
    });
  });