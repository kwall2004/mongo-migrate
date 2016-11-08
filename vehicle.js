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

connection.query('SELECT v.VhclID, v.BsnsInfoID, v.Make, v.Modl, v.ModlYear, v.Alas, v.InitOdo, v.VIN ' +
  'FROM Vhcl v ', function (err, rows, fields) {
    if (err) throw err;

    MongoClient.connect(uri).then(function (db) {
      var clients = db.collection('clients');
      var vehicles = db.collection('vehicles');

      Promise.each(rows, function (row) {
        var doc = {
          oldId: row.VhclID,
          clientId: null,
          make: row.Make,
          model: row.Modl,
          modelYear: row.ModlYear,
          alias: row.Alas,
          odometer: row.InitOdo,
          vin: row.VIN
        };

        return clients.findOne({ oldId: parseInt(row.BsnsInfoID) }).then(function (client) {
          if (client) {
            doc.clientId = ObjectID(client._id);
          }

          return vehicles.insertOne(doc).then(function (result) {
            console.log(row.VhclID, result.result);

          }).catch(function (err) {
            throw err;
          });

        }).catch(function (err) {
          throw err;
        });

      }).then(function (result) {
        console.log('done');
        return;

      }).catch(function (err) {
        throw err;
      });

    }).catch(function (err) {
      throw err;
    });
  });
