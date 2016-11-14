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

        return Promise
          .each(rows, function (row, index) {
            return devices
              .findOne({ oldId: parseInt(row.DvceID) })

              .then(function (device) {
                if (device) {
                  return clients
                    .findOne({ oldId: parseInt(row.BsnsInfoID) })

                    .then(function (client) {
                      if (client) {
                        if (!row.EndDate) {
                          if (device.currentClient) {
                            var startDate = new Date(row.StrtDate * 1000);
                            if (startDate > device.currentClient.startDate) {
                              return devices
                                .updateOne({ _id: device._id }, {
                                  $push: {
                                    previousClients: {
                                      id: device.currentClient.id,
                                      startDate: device.currentClient.startDate,
                                      endDate: device.currentClient.endDate
                                    }
                                  }
                                })

                                .then(function () {
                                  return devices.updateOne({ _id: device._id }, {
                                    $set: {
                                      currentClient: {
                                        id: ObjectID(client._id),
                                        startDate: startDate
                                      }
                                    }
                                  });
                                });
                            }
                            else {
                              return devices.updateOne({ _id: device._id }, {
                                $push: {
                                  previousClients: {
                                    id: ObjectID(client._id),
                                    startDate: new Date(row.StrtDate * 1000),
                                    endDate: new Date(row.EndDate * 1000)
                                  }
                                }
                              });
                            }
                          }

                          return devices.updateOne({ _id: device._id }, {
                            $set: {
                              currentClient: {
                                id: ObjectID(client._id),
                                startDate: new Date(row.StrtDate * 1000)
                              }
                            }
                          });
                        }
                        else {
                          return devices.updateOne({ _id: device._id }, {
                            $push: {
                              previousClients: {
                                id: ObjectID(client._id),
                                startDate: new Date(row.StrtDate * 1000),
                                endDate: new Date(row.EndDate * 1000)
                              }
                            }
                          });
                        }
                      }
                    })

                    .then(function () {
                      return vehicles.findOne({ oldId: parseInt(row.VhclID) });
                    })

                    .then(function (vehicle) {
                      if (vehicle) {
                        if (!row.EndDate) {
                          if (device.currentVehicle) {
                            var startDate = new Date(row.StrtDate * 1000);
                            if (startDate > device.currentVehicle.startDate) {
                              return devices
                                .updateOne({ _id: device._id }, {
                                  $push: {
                                    previousVehicles: {
                                      id: device.currentVehicle.id,
                                      startDate: device.currentVehicle.startDate,
                                      endDate: device.currentVehicle.endDate
                                    }
                                  }
                                })

                                .then(function () {
                                  return devices.updateOne({ _id: device._id }, {
                                    $set: {
                                      currentVehicle: {
                                        id: ObjectID(vehicle._id),
                                        startDate: startDate
                                      }
                                    }
                                  });
                                });
                            }
                            else {
                              return devices.updateOne({ _id: device._id }, {
                                $push: {
                                  previousVehicles: {
                                    id: ObjectID(vehicle._id),
                                    startDate: new Date(row.StrtDate * 1000),
                                    endDate: new Date(row.EndDate * 1000)
                                  }
                                }
                              });
                            }
                          }

                          return devices.updateOne({ _id: device._id }, {
                            $set: {
                              currentVehicle: {
                                id: ObjectID(vehicle._id),
                                startDate: new Date(row.StrtDate * 1000)
                              }
                            }
                          });
                        }
                        else {
                          return devices.updateOne({ _id: device._id }, {
                            $push: {
                              previousVehicles: {
                                id: ObjectID(vehicle._id),
                                startDate: new Date(row.StrtDate * 1000),
                                endDate: row.EndDate ? new Date(row.EndDate * 1000) : null
                              }
                            }
                          });
                        }
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
                    serialNumber: row.SrlNum ? row.SrlNum.toString() : null,
                    createdAt: new Date()
                  };

                  return clients.findOne({ oldId: parseInt(row.BsnsInfoID) })
                    .then(function (client) {
                      if (client) {
                        if (!row.EndDate) {
                          doc.currentClient = {
                            id: ObjectID(client._id),
                            startDate: new Date(row.StrtDate * 1000)
                          }
                        }
                        else {
                          if (!doc.previousClients) doc.previousClients = [];

                          doc.previousClients.push({
                            id: ObjectID(client._id),
                            startDate: new Date(row.StrtDate * 1000),
                            endDate: row.EndDate ? new Date(row.EndDate * 1000) : null
                          });
                        }
                      }

                      return vehicles.findOne({ oldId: parseInt(row.VhclID) })
                    })

                    .then(function (vehicle) {
                      if (vehicle) {
                        if (!row.EndDate) {
                          doc.currentVehicle = {
                            id: ObjectID(vehicle._id),
                            startDate: new Date(row.StrtDate * 1000)
                          }
                        }
                        else {
                          if (!doc.previousVehicles) doc.previousVehicles = [];

                          doc.previousVehicles.push({
                            id: ObjectID(vehicle._id),
                            startDate: new Date(row.StrtDate * 1000),
                            endDate: row.EndDate ? new Date(row.EndDate * 1000) : null
                          });
                        }
                      }

                      return devices.insertOne(doc)
                    });
                }
              })

              .then(function (result) {
                console.log(index, result.result);
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
