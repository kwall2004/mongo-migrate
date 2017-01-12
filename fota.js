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

connection.query('SELECT CmndDataID, DvceID, SoftVer, HrdwVer, CretTime, ExecTime, Cmnd, Stat, ExecStts, Arg1, Arg2, Arg3 ' +
  'FROM FOTACmnd ', function (err, rows, fields) {
    if (err) throw err;

    MongoClient
      .connect(uri)

      .then(function (db) {
        var devices = db.collection('devices');
        var fotaCommands = db.collection('fota-commands');

        return Promise
          .each(rows, function (row, index) {
            var doc = {
              oldId: row.CmndDataID,
              deviceId: null,
              softwareVersion: row.SoftVer,
              hardwareVersion: row.HrdwVer,
              createdDate: row.CretTime,
              executedDate: row.ExecTime,
              command: row.Cmnd,
              state: row.Stat,
              executionStatus: row.ExecStts,
              arg1: row.Arg1,
              arg2: row.Arg2,
              arg3: row.Arg3
            };

            return devices
              .findOne({ oldId: parseInt(row.DvceID) })

              .then(function (device) {
                if (device) {
                  doc.deviceId = ObjectID(device._id);
                }

                return fotaCommands.insertOne(doc).then(function (result) {
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
