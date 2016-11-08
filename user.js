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

connection.query('SELECT u.UserInfoID, u.BsnsInfoID, u.LognID, u.UserName, u.Email, m.DvceID, unix_timestamp(m.StrtDate) AS StrtDate, unix_timestamp(m.EndDate) AS EndDate ' +
  'FROM UserInfo u ' +
  'LEFT JOIN UserVhclDvceMap m ON m.UserInfoID = u.UserInfoID ', function (err, rows, fields) {
    if (err) throw err;

    MongoClient.connect(uri).then(function (db) {
      var clients = db.collection('clients');
      var devices = db.collection('devices');
      var users = db.collection('users');

      Promise.each(rows, function (row) {
        return users.findOne({ oldId: parseInt(row.UserInfoID) }).then(function (user) {
          if (user) {
            return devices.findOne({ oldId: parseInt(row.DvceID) }).then(function (device) {
              if (device) {
                return users.updateOne({ _id: user._id }, {
                  $push: {
                    devices: {
                      id: ObjectID(device._id),
                      startDate: new Date(row.StrtDate * 1000),
                      endDate: row.EndDate ? new Date(row.EndDate * 1000) : null
                    }
                  }

                }).then(function (result) {
                  console.log(row.UserInfoID, result.result);

                }).catch(function (err) {
                  throw err;
                });
              }

            }).catch(function (err) {
              throw err;
            });
          }
          else {
            var doc = {
              oldId: row.UserInfoID,
              clientId: null,
              loginId: row.LognID,
              userName: row.UserName,
              email: row.Email
            };

            return clients.findOne({ oldId: row.BsnsInfoID }).then(function (client) {
              if (client) {
                doc.clientId = ObjectID(client._id);
              }

              return devices.findOne({ oldId: parseInt(row.DvceID) }).then(function (device) {
                if (device) {
                  doc.devices = [];
                  doc.devices.push({
                    id: ObjectID(device._id),
                    startDate: new Date(row.StrtDate * 1000),
                    endDate: row.EndDate ? new Date(row.EndDate * 1000) : null
                  });
                }

                return users.insertOne(doc).then(function (result) {
                  console.log(row.UserInfoID, result.result);

                }).catch(function (err) {
                  throw err;
                });

              }).catch(function (err) {
                throw err;
              });

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

    }).catch(function (err) {
      throw err;
    });
  });
