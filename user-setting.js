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

connection.query('SELECT us.UserSettingId, au.UserInfoId, t.Type, us.Key, us.Value ' +
  'FROM UserSettings us ' +
  'LEFT JOIN AspNetUsers au ON au.Id = us.UserId ' +
  'LEFT JOIN UserSettingTypes t ON t.UserSettingTypeId = us.UserSettingTypeId', function (err, rows, fields) {
    if (err) throw err;

    MongoClient.connect(uri).then(function (db) {
      var users = db.collection('users');
      var vehicles = db.collection('vehicles');
      var settings = db.collection('user-settings');

      Promise.each(rows, function (row) {
        var doc = {
          oldId: row.UserSettingId,
          userId: null,
          key: row.Key,
          value: row.Value
        };

        return users.findOne({ oldId: row.UserInfoId }).then(function (user) {
          if (user) {
            doc.userId = ObjectID(user._id);
          }

          if (!isNaN(row.Key)) {
            return vehicles.findOne({ oldId: parseInt(row.Key) }).then(function (vehicle) {
              if (vehicle) {
                doc.key = row.Type;
                doc.vehicleId = ObjectID(vehicle._id);
              }

              return settings.insertOne(doc).then(function (result) {
                console.log(row.UserSettingId, result.result);

              }).catch(function (err) {
                throw err;
              });

            }).catch(function (err) {
              throw err;
            });
          }
          else {
            if (row.Key.startsWith('Default')) {
              doc.key = row.Key.replace('Default', '');
            }

            return settings.insertOne(doc).then(function (result) {
              console.log(row.UserSettingId, result.result);

            }).catch(function (err) {
              throw err;
            });
          }

        }).catch(function (err) {
          throw err;
        });

      }).then(function (result) {
        console.log('done');
        return;

      }).catch(function (err) {
        throw err;
      });
    });
  });