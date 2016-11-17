var mysql = require('mysql');
var MongoClient = require('mongodb').MongoClient;
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

connection.query('SELECT DvceActnStngID, GrupID, LogrEvnt, Indx, Actn, Arg1, Arg2, Arg3 ' +
  'FROM DvceActnStng', function (err, rows) {
    if (err) throw err;

    MongoClient.connect(uri).then(function (db) {
      var deviceActions = db.collection('device-actions');

      Promise.each(rows, function (row) {
        var doc = {
          oldId: row.DvceActnStngID,
          groupId: row.GrupID,
          logEvent: row.LogrEvnt,
          index: row.Indx,
          action: row.Actn,
          arg1: row.Arg1,
          arg2: row.Arg2,
          arg3: row.Arg3
        };

        return deviceActions.insertOne(doc).then(function (result) {
          console.log(row.DvceActnStngID, result.result);

        }).catch(function (err) {
          throw err;
        });

      }).then(function () {
        console.log('done');
        return;

      }).catch(function (err) {
        throw err;
      });
    });
  });