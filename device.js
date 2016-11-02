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

connection.query('SELECT d.DvceID, d.BsnsInfoID, bi.BsnsName, d.IMEI, d.GrupID, d.FWVrsn, d.ConfVrsn, d.SrlNum ' +
  'FROM Dvce d ' +
  'LEFT JOIN BsnsInfo bi ON bi.BsnsInfoID = d.BsnsInfoID', function (err, rows, fields) {
    if (err) throw err;

    MongoClient.connect(uri, function (err, db) {
      if (err) throw err;

      var clients = db.collection('clients');
      var devices = db.collection('devices');

      function processRow(rows, i) {
        if (i == rows.length) {
          console.log('done');
          return;
        }

        var row = rows[i];

        var doc = {
          OldId: row.DvceID,
          ClientId: null,
          ClientName: row.BsnsName,
          IMEI: row.IMEI.toString(),
          GroupId: parseInt(row.GrupID),
          FirmwareVersion: row.FWVrsn,
          ConfigVersion: row.ConfVrsn,
          SerialNumber: row.SrlNum ? row.SrlNum.toString() : null
        };

        clients.findOne({ OldId: row.BsnsInfoID }, function (err, client) {
          if (err) throw err;

          if (client) {
            doc.ClientId = client._id.toString();
          }

          devices.insertOne(doc, function (err, result) {
            if (err) throw err;

            console.log(i, result.result);

            processRow(rows, i + 1);
          });
        });
      }

      processRow(rows, 0);
    });
  });