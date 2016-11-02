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

connection.query('SELECT u.UserInfoID, u.BsnsInfoID, bi.BsnsName, u.LognID, u.UserName, u.Email ' +
  'FROM UserInfo u ' +
  'LEFT JOIN BsnsInfo bi ON bi.BsnsInfoID = u.BsnsInfoID', function (err, rows, fields) {
    if (err) throw err;

    MongoClient.connect(uri, function (err, db) {
      if (err) throw err;

      var clients = db.collection('clients');
      var users = db.collection('users');

      function processRow(rows, i) {
        if (i == rows.length) {
          console.log('done');
          return;
        }

        var row = rows[i];

        var doc = {
          OldId: row.UserInfoID,
          ClientId: null,
          ClientName: row.BsnsName,
          LoginId: row.LognID,
          UserName: row.UserName,
          Email: row.Email
        };

        clients.findOne({ OldId: row.BsnsInfoID }, function (err, client) {
          if (err) throw err;

          if (client) {
            doc.ClientId = client._id.toString();
          }

          users.insertOne(doc, function (err, result) {
            if (err) throw err;

            console.log(i, result.result);

            processRow(rows, i + 1);
          });
        });
      }

      processRow(rows, 0);
    });
  });
