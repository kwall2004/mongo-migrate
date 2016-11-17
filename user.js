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

connection.query('SELECT u.UserInfoID, u.BsnsInfoID, u.LognID, u.UserName, u.Email ' +
  'FROM UserInfo u ', function (err, rows) {
    if (err) throw err;

    MongoClient
      .connect(uri)

      .then(function (db) {
        var clients = db.collection('clients');
        var users = db.collection('users');

        return Promise
          .each(rows, function (row, index) {
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

              return users.insertOne(doc).then(function (result) {
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