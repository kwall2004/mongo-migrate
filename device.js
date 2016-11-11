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

connection.query('SELECT d.DvceID, d.IMEI, d.GrupID, d.FWVrsn, d.ConfVrsn, d.SrlNum, ui.BsnsInfoID, m.VhclID, unix_timestamp(m.StrtDate) AS StrtDate, unix_timestamp(m.EndDate) AS EndDate ' +
  'FROM Dvce d ' +
  'LEFT JOIN UserVhclDvceMap m ON m.DvceID = d.DvceID ' +
  'LEFT JOIN UserInfo ui ON ui.UserInfoID = m.UserInfoID ', function (err, rows, fields) {
    if (err) throw err;

    MongoClient.connect(uri)
      .then(function (db) {
        var clients = db.collection('clients');
        var vehicles = db.collection('vehicles');
        var devices = db.collection('devices');

        return Promise.each(rows, function (row) {
          return devices.findOne({ oldId: parseInt(row.DvceID) })
            .then(function (device) {
              if (device) {
                return clients.findOne({ oldId: parseInt(row.BsnsInfoID) })
                  .then(function (client) {
                    if (client) {
                      return devices.updateOne({ _id: device._id }, {
                        $push: {
                          clients: {
                            id: ObjectID(client._id),
                            startDate: new Date(row.StrtDate * 1000),
                            endDate: row.EndDate ? new Date(row.EndDate * 1000) : null
                          }
                        }
                      });
                    }
                  })

                  .then(function () {
                    return vehicles.findOne({ oldId: parseInt(row.VhclID) });
                  })

                  .then(function (vehicle) {
                    if (vehicle) {
                      return devices.updateOne({ _id: device._id }, {
                        $push: {
                          vehicles: {
                            id: ObjectID(vehicle._id),
                            startDate: new Date(row.StrtDate * 1000),
                            endDate: row.EndDate ? new Date(row.EndDate * 1000) : null
                          }
                        }
                      });
                    }
                  });
              }
              else {
                var doc = {
                  oldId: row.DvceID,
                  imei: row.IMEI.toString(),
                  groupId: parseInt(row.GrupID),
                  firmwareVersion: row.FWVrsn,
                  configVersion: row.ConfVrsn,
                  serialNumber: row.SrlNum ? row.SrlNum.toString() : null
                };

                return clients.findOne({ oldId: parseInt(row.BsnsInfoID) })
                  .then(function (client) {
                    if (client) {
                      if (!doc.clients) doc.clients = [];

                      doc.clients.push({
                        id: ObjectID(client._id),
                        startDate: new Date(row.StrtDate * 1000),
                        endDate: row.EndDate ? new Date(row.EndDate * 1000) : null
                      })
                    }

                    return vehicles.findOne({ oldId: parseInt(row.VhclID) })
                  })

                  .then(function (vehicle) {
                    if (vehicle) {
                      if (!doc.vehicles) doc.vehicles = [];

                      doc.vehicles.push({
                        id: ObjectID(vehicle._id),
                        startDate: new Date(row.StrtDate * 1000),
                        endDate: row.EndDate ? new Date(row.EndDate * 1000) : null
                      });
                    }

                    return devices.insertOne(doc)
                  });
              }
            })

            .then(function (result) {
              console.log(row.DvceID, result.result);
            });
        })

          .then(function (result) {
            console.log('done');
            return;
          })
      })

      .catch(function (err) {
        throw err;
      });
  });
