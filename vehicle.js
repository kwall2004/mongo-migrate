var mysql = require('mysql');
var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectID;
var Promise = require('bluebird');

// var uri = 'mongodb://localhost:27017/vision2';
var uri = 'mongodb://vis2devmongo01.danlawinc.com:27017/local';

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

    MongoClient
      .connect(uri)

      .then(function (db) {
        var clients = db.collection('clients');
        var vehicles = db.collection('vehicles');

        return Promise
          .each(rows, function (row, index) {
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

            return clients
              .findOne({ oldId: parseInt(row.BsnsInfoID) })

              .then(function (client) {
                if (client) {
                  doc.clientId = ObjectID(client._id);
                }

                return vehicles.insertOne(doc).then(function (result) {
                  console.log(index, result.result);
                });
              });
          })

          .then(function () {
            db.close();
            console.log('done');
          });
      })

      .catch(function (err) {
        throw err;
      });
  });
