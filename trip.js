var MongoClient = require('mongodb').MongoClient;
var request = require('request');
var elasticsearch = require('elasticsearch');
var Promise = require('bluebird');

// var uri = 'mongodb://localhost:27017/vision2';
var uri = 'mongodb://heroku_9d3ppsr0:a706flgp82q7qenmd166qmvq8d@ds051655.mlab.com:51655/heroku_9d3ppsr0';

var token = 'hbTXE8GxVjiqOBZBmnirt5LQLyGkFaQAOcpDJs60EwERbm778C_qC9TlnPBYb5lbmOPjAxQTXFAERYqS7W3bwIVzut7GPhcIYUsJ9mQmXdTDzWY0cTg8ZJFL6JLPJQEGQHL5I4_0J3FZu8cihO2Xgo1TZvGbZSfxG-ALkiDoO_1UY4FetP27inBLKh1HGiMtEnzgzH6CdntAoKJzapgk7tRq9OSCutdHGPgB-4E1sjifrImQAhT56j3YOC0XDbFF4LsX8Q_4VvbB0_rkYzYTcxxy3t-K18UcGMtMHzIl9dXqPuaPEbM5nvMiaxe_JptmYcInvEcWMvPumtCGfjPoKkJ1bgGPpEzNmEMihn5i06-IMcKplB_kJ8coGzFgl8DnRCfvkrPMYr5pFhE9iXBMqcr7IsI8s85fZXf19t9n_TSSsu6-tqJfW1ebVpkt3zWdo-1RSZEPiMaFh1tnTUE5r3-DjCB1SJLlp87TgQdXdtCv_25tvhe2FDa9QDEzg0C7PI6E7QthdLlcEZBYjmrJIefkl4ZW-UNZS8zis-ziSn3aB6NWsuEhOFTtn8lNGL8bZPDGfMvsARorpmMK4NjGj4eX2ZvLOZUzrRcupqUiSk_q8Q6YiW0U_pHQZJey9qmQ7Ntn3lHyNWUHhKqFsI8pYx6N72SljR9lmRDQf3ogKoQ3eREmc2WEmwG13XMVFVW7Y0XuW2WkSeHdlZuYmGsV_5iZZLwd21cmO0Vz3OcI7BFwS6SjBLTAPhFVr8Gzctf1WV75sPnE5EpuPnl4SAxP5bqTrpWp92aawaPjDrj7Id5Mzacm3EKY16AqlZph9JMf1i0pgZdBepPXmtuKFF3_bvFyCPtX24n03HdEmTy4BpcS1QrQCoyQohumM8Tj2UR_0dBVdmSh7V--K9dv8hyUMTFfivW6X9EzdIw6OQi6DnsDHQLDG18ETs2k0HLO_RTOOsaMX-HmGNgQAsK54QDlxgSBXCN5tf5juBAFxVggWpxxzmIo6FyprJLJlnrYscTOyPtM56x6DCuCqcP7n5xCvnE-bUqduVEXNUgu1sLk7N--gNjl9Z04Z-HEuA_1w8YurTsxauLdSFlIlevRLTVaVGydyZpOyarnVZMkZVfrmo-MEj8SXW-_d4eXGwxlL8Niu6g2PB7ihFDHuws1szog0tiITHzGjIHZnRaaLcieBNHssnWRrVIfFzJ0zM-h4Qc7JdwIYv41AIYNgh9jH_i7vdMFVZhFt77oJFYo_yROF4IW83sXpsXDu0tw5QU2MkVEBv2Rai9rq6ChRD406gcNvIjuEED59y20ojEdlyBkxKA5nD93PMrR_NlrwOCF18_Dmcg6M-AfhkiX-sNDynZnCJzvxfdnpu1GmTr09L1e9LbWNfgIQjY5EveuAWqh0D7MARsFGPppxqy1YDY8HUEO498rmKGX_9TWSn4Nb7DAjdgtJhJZa4p3gMd7Dj7D7qE-wpH7ECAqce8_XG8vchVz20P7_A8NvQbCOXAGbot45-hFNLwicG3jCAOqozqrvNTCNvUYxX1m91DPOsXbQoGDwO5D0fyVlAL89zgtCVeM09JAK5MOVbKXFsp7efbq7Xpo';

var client = new elasticsearch.Client({
  host: 'https://lo0l35z0:6mnvwo0j46ih3gym@fig-7632293.us-east-1.bonsaisearch.net'
});

client.indices
  .exists({ index: 'vision' })

  .then(function (response) {
    if (!response) {
      return client.indices.create(
        {
          index: 'vision',
          body: {
            mappings: {
              message: {
                properties: {
                  m12: {
                    type: 'double'
                  },
                  m13: {
                    type: 'double'
                  },
                  x: {
                    type: 'double'
                  },
                  y: {
                    type: 'double'
                  },
                  z: {
                    type: 'double'
                  },
                  l: {
                    type: 'double'
                  },
                  n: {
                    type: 'double'
                  }
                }
              }
            }
          }
        }
      );
    }
  })

  .then(function () {
    var vehicleId = '105342';
    // var vehicleId = process.argv[2];
    var date = '2016-05-20';

    return MongoClient
      .connect(uri)

      .then(function (db) {
        var vehiclesCollection = db.collection('vehicles');
        var devicesCollection = db.collection('devices');

        return vehiclesCollection
          .findOne({ oldId: parseInt(vehicleId) })

          .then(function (vehicle) {
            if (vehicle) {
              return new Promise(function (resolve, reject) {
                request.get(
                  {
                    url: 'http://visiontest.danlawinc.com/api/Trips/GetTrips?VehicleId=' + vehicleId + '&SelectedDate=' + date,
                    auth: {
                      bearer: token
                    }
                  },
                  function (error, response, body) {
                    if (error) {
                      reject(error);
                      return;
                    }

                    var trips = JSON.parse(body);

                    Promise
                      .each(trips, function (trip) {
                        console.log(trip.trip.tripId, trip.trip.deviceId);

                        return devicesCollection
                          .findOne({ oldId: parseInt(trip.trip.deviceId) })

                          .then(function (device) {
                            return new Promise(function (resolve, reject) {
                              request.get(
                                {
                                  url: 'http://visiontest.danlawinc.com/api/Trips/GetTripJsonData?TripId=' + trip.trip.tripId,
                                  auth: {
                                    bearer: token
                                  }
                                },
                                function (error, response, body) {
                                  if (error) {
                                    reject(error);
                                    return;
                                  };

                                  if (response.statusCode == 200) {
                                    var data = JSON.parse(body);

                                    return Promise
                                      .each(data, function (element, index) {
                                        var body = Object.assign(
                                          {
                                            deviceId: device._id.toString(),
                                            vehicleId: vehicle._id.toString(),
                                            tripId: trip.trip.tripId.toString(),
                                            tripNumber: trip.trip.tripNumber.toString()
                                          },
                                          element
                                        );

                                        return client
                                          .index(
                                          {
                                            index: 'vision',
                                            type: 'message',
                                            body: body
                                          })

                                          .then(function () {
                                            console.log(index);
                                          })
                                      })

                                      .then(function () {
                                        resolve();
                                      })

                                      .catch(function (err) {
                                        reject(err);
                                      });
                                  }
                                }
                              );
                            })
                          })
                      })

                      .then(function () {
                        console.log('done');
                        resolve();
                      })

                      .catch(function (err) {
                        reject(err);
                      });
                  }
                );
              });
            }
          })

          .finally(function () {
            db.close();
          });
      });
  })

  .catch(function (err) {
    throw err;
  });

