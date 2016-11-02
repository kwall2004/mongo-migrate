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

connection.query('SELECT v.VhclID, v.BsnsInfoID, b.BsnsName, m.UserInfoID, v.Make, v.Modl, v.ModlYear, v.Alas, v.InitOdo ' +
  'FROM Vhcl v ' +
  'LEFT JOIN UserVhclDvceMap m ON m.VhclID = v.VhclID AND (m.EndDate IS NULL OR m.EndDate > NOW()) ' +
  'LEFT JOIN BsnsInfo b ON b.BsnsInfoID = v.BsnsInfoID', function (err, rows, fields) {
    if (err) throw err;

    MongoClient.connect(uri, function (err, db) {
      if (err) throw err;

      var clients = db.collection('clients');
      var users = db.collection('users');
      var vehicles = db.collection('vehicles');

      function processRow(rows, i) {
        if (i == rows.length) {
          console.log('done');
          return;
        }

        var row = rows[i];

        vehicles.findOne({ OldId: row.VhclID }, function (err, vehicle) {
          if (err) throw err;

          if (vehicle) {
            users.findOne({ OldId: row.UserInfoID }, function (err, user) {
              if (err) throw err;

              if (user) {
                vehicles.updateOne({ _id: vehicle._id }, {
                  $push: {
                    Users: {
                      id: user._id.toString(),
                      loginId: user.LoginId,
                      userName: user.UserName
                    }
                  }
                }, function (err, result) {
                  if (err) throw err;

                  console.log(i, result.result);

                  processRow(rows, i + 1);
                });
              }
            });
          }
          else {
            var doc = {
              OldId: row.VhclID,
              ClientId: null,
              ClientName: row.BsnsName,
              Users: [],
              Make: row.Make,
              Model: row.Modl,
              ModelYear: row.ModlYear,
              Alias: row.Alas,
              Odometer: row.InitOdo
            };

            clients.findOne({ OldId: row.BsnsInfoID }, function (err, client) {
              if (err) throw err;

              if (client) {
                doc.ClientId = client._id.toString();
              }

              users.findOne({ OldId: row.UserInfoID }, function (err, user) {
                if (err) throw err;

                if (user) {
                  doc.Users.push({
                    id: user._id.toString(),
                    loginId: user.LoginId,
                    userName: user.UserName
                  });
                }

                vehicles.insertOne(doc, function (err, result) {
                  if (err) throw err;

                  console.log(i, result.result);

                  processRow(rows, i + 1);
                });
              });
            });
          }
        });
      }

      processRow(rows, 0);
    });
  });