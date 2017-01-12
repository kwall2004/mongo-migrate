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

connection.query('SELECT d.DvceID, d.IMEI, d.GrupID, d.FWVrsn, d.ConfVrsn, d.SrlNum, ui.BsnsInfoID, m.VhclID, unix_timestamp(m.StrtDate) AS StrtDate, unix_timestamp(m.EndDate) AS EndDate ' +
  'FROM Dvce d ' +
  'LEFT JOIN UserVhclDvceMap m ON m.DvceID = d.DvceID ' +
  'LEFT JOIN UserInfo ui ON ui.UserInfoID = m.UserInfoID ', function (err, rows) {
    if (err) throw err;

    MongoClient.connect(uri)
      .then(function (db) {
        var clientsCollection = db.collection('clients');
        var vehiclesCollection = db.collection('vehicles');
        var devicesCollection = db.collection('devices');

        return Promise
          .each(rows, function (row, index) {
            return devicesCollection
              .findOne({ oldId: parseInt(row.DvceID) })

              .then(function (device) {
                if (device) {
                  return clientsCollection
                    .findOne({ oldId: parseInt(row.BsnsInfoID) })

                    .then(function (client) {
                      if (client) {
                        if (!row.EndDate) {
                          if (device.currentClient) {
                            var startDate = new Date(row.StrtDate * 1000);
                            if (startDate > device.currentClient.startDate) {
                              return devicesCollection
                                .updateOne({ _id: device._id }, {
                                  $push: {
                                    previousClients: {
                                      id: device.currentClient.id,
                                      startDate: device.currentClient.startDate,
                                      endDate: startDate
                                    }
                                  }
                                })

                                .then(function () {
                                  return devicesCollection.updateOne({ _id: device._id }, {
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
                              return devicesCollection.updateOne({ _id: device._id }, {
                                $push: {
                                  previousClients: {
                                    id: ObjectID(client._id),
                                    startDate: startDate,
                                    endDate: device.currentClient.startDate
                                  }
                                }
                              });
                            }
                          }
                          else {
                            return devicesCollection.updateOne({ _id: device._id }, {
                              $set: {
                                currentClient: {
                                  id: ObjectID(client._id),
                                  startDate: startDate
                                }
                              }
                            });
                          }
                        }
                        else {
                          return devicesCollection.updateOne({ _id: device._id }, {
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
                      return vehiclesCollection.findOne({ oldId: parseInt(row.VhclID) });
                    })

                    .then(function (vehicle) {
                      if (vehicle) {
                        if (!row.EndDate) {
                          var startDate = new Date(row.StrtDate * 1000);
                          var endDate = null;

                          return new Promise(function (resolve, reject) {
                            devicesCollection
                              .find({ 'currentVehicle.id': vehicle._id, _id: { $ne: device._id } })

                              .toArray(function (err, devices) {
                                if (err) reject(err);

                                Promise
                                  .each(devices, function (d) {
                                    if (startDate >= d.currentVehicle.startDate) {
                                      return devicesCollection
                                        .updateOne({ _id: d._id }, {
                                          $push: {
                                            previousVehicles: {
                                              id: d.currentVehicle.id,
                                              startDate: d.currentVehicle.startDate,
                                              endDate: startDate
                                            }
                                          }
                                        })

                                        .then(function () {
                                          return devicesCollection.updateOne({ _id: d._id }, {
                                            $unset: {
                                              vehicleAlias: '',
                                              currentVehicle: ''
                                            }
                                          });
                                        });
                                    }
                                    else {
                                      endDate = d.currentVehicle.startDate;
                                    }
                                  })

                                  .then(function () {
                                    if (!endDate) {
                                      if (device.currentVehicle) {
                                        if (startDate >= device.currentVehicle.startDate) {
                                          return devicesCollection
                                            .updateOne({ _id: device._id }, {
                                              $push: {
                                                previousVehicles: {
                                                  id: device.currentVehicle.id,
                                                  startDate: device.currentVehicle.startDate,
                                                  endDate: startDate
                                                }
                                              }
                                            })

                                            .then(function () {
                                              return devicesCollection.updateOne({ _id: device._id }, {
                                                $set: {
                                                  vehicleAlias: vehicle.alias,
                                                  currentVehicle: {
                                                    id: ObjectID(vehicle._id),
                                                    startDate: startDate
                                                  }
                                                }
                                              });
                                            });
                                        }
                                        else {
                                          return devicesCollection.updateOne({ _id: device._id }, {
                                            $push: {
                                              previousVehicles: {
                                                id: ObjectID(vehicle._id),
                                                startDate: startDate,
                                                endDate: device.currentVehicle.startDate
                                              }
                                            }
                                          });
                                        }
                                      }
                                      else {
                                        return devicesCollection.updateOne({ _id: device._id }, {
                                          $set: {
                                            vehicleAlias: vehicle.alias,
                                            currentVehicle: {
                                              id: ObjectID(vehicle._id),
                                              startDate: startDate
                                            }
                                          }
                                        });
                                      }
                                    }
                                    else {
                                      return devicesCollection.updateOne({ _id: device._id }, {
                                        $push: {
                                          previousVehicles: {
                                            id: ObjectID(vehicle._id),
                                            startDate: new Date(row.StrtDate * 1000),
                                            endDate: endDate
                                          }
                                        }
                                      });
                                    }
                                  })

                                  .then(function () {
                                    resolve();
                                  })

                                  .catch(function (err) {
                                    reject(err);
                                  });
                              });
                          });
                        }
                        else {
                          return devicesCollection.updateOne({ _id: device._id }, {
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

                  return clientsCollection.findOne({ oldId: parseInt(row.BsnsInfoID) })
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
                            endDate: new Date(row.EndDate * 1000)
                          });
                        }
                      }

                      return vehiclesCollection.findOne({ oldId: parseInt(row.VhclID) })
                    })

                    .then(function (vehicle) {
                      if (vehicle) {
                        if (!row.EndDate) {
                          var startDate = new Date(row.StrtDate * 1000);

                          doc.vehicleAlias = vehicle.alias;
                          doc.currentVehicle = {
                            id: ObjectID(vehicle._id),
                            startDate: new Date(row.StrtDate * 1000)
                          };

                          return new Promise(function (resolve, reject) {
                            devicesCollection
                              .find({ 'currentVehicle.id': vehicle._id })

                              .toArray(function (err, devices) {
                                if (err) reject(err);

                                Promise
                                  .each(devices, function (d) {
                                    if (startDate >= d.currentVehicle.startDate) {
                                      return devicesCollection
                                        .updateOne({ _id: d._id }, {
                                          $push: {
                                            previousVehicles: {
                                              id: d.currentVehicle.id,
                                              startDate: d.currentVehicle.startDate,
                                              endDate: startDate
                                            }
                                          }
                                        })

                                        .then(function () {
                                          return devicesCollection.updateOne({ _id: d._id }, {
                                            $unset: {
                                              vehicleAlias: '',
                                              currentVehicle: ''
                                            }
                                          });
                                        });
                                    }
                                    else {
                                      delete doc.vehicleAlias;
                                      delete doc.currentVehicle;

                                      if (!doc.previousVehicles) doc.previousVehicles = [];

                                      doc.previousVehicles.push({
                                        id: ObjectID(vehicle._id),
                                        startDate: new Date(row.StrtDate * 1000),
                                        endDate: d.currentVehicle.startDate
                                      });
                                    }
                                  })

                                  .then(function () {
                                    resolve();
                                  })

                                  .catch(function (err) {
                                    reject(err);
                                  });
                              });
                          });
                        }
                        else {
                          if (!doc.previousVehicles) doc.previousVehicles = [];

                          doc.previousVehicles.push({
                            id: ObjectID(vehicle._id),
                            startDate: new Date(row.StrtDate * 1000),
                            endDate: new Date(row.EndDate * 1000)
                          });
                        }
                      }
                    })

                    .then(function () {
                      return devicesCollection.insertOne(doc);
                    });
                }
              })

              .then(function () {
                console.log(index);
              });
          })

          .then(function () {
            console.log('done');
            return;
          })
      })

      .catch(function (err) {
        throw err;
      });
  });
