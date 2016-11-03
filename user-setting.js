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

connection.query('SELECT us.UserSettingId, au.UserInfoId, t.Type, us.Key, us.Value ' +
  'FROM UserSettings us ' +
  'LEFT JOIN AspNetUsers au ON au.Id = us.UserId ' +
  'LEFT JOIN UserSettingTypes t ON t.UserSettingTypeId = us.UserSettingTypeId', function (err, rows, fields) {
    if (err) throw err;

    MongoClient.connect(uri, function (err, db) {
      if (err) throw err;

      var users = db.collection('users');
      var vehicles = db.collection('vehicles');
      var settings = db.collection('user-settings');

      function processRow(rows, i) {
        if (i == rows.length) {
          console.log('done');
          return;
        }

        var row = rows[i];

        var doc = {
          OldId: row.UserSettingId,
          UserId: null,
          Key: row.Key,
          Value: row.Value
        };

        users.findOne({ OldId: row.UserInfoId }, function (err, user) {
          if (err) throw err;

          if (user) {
            doc.UserId = user._id.toString();
          }

          if (!isNaN(row.Key)) {
            vehicles.findOne({ OldId: parseInt(row.Key) }, function (err, vehicle) {
              if (err) throw err;

              if (vehicle) {
                doc.Key = row.Type;
                doc.VehicleId = vehicle._id.toString();
              }

              settings.insertOne(doc, function (err, result) {
                if (err) throw err;

                console.log(i, result.result);

                processRow(rows, i + 1);
              });
            });
          }
          else {
            if (row.Key.startsWith('Default')) {
              doc.Key = row.Key.replace('Default', '');
            }

            settings.insertOne(doc, function (err, result) {
              if (err) throw err;

              console.log(i, result.result);

              processRow(rows, i + 1);
            });
          }
        });
      }

      processRow(rows, 0);
    });
  });