var MongoClient = require('mongodb').MongoClient;
var request = require('request');
var elasticsearch = require('elasticsearch');
var Promise = require('bluebird');

// var uri = 'mongodb://localhost:27017/vision2';
var uri = 'mongodb://heroku_9d3ppsr0:a706flgp82q7qenmd166qmvq8d@ds051655.mlab.com:51655/heroku_9d3ppsr0';

var token = 'XGl1bCwvWutfv6zsdXSEsPxuJN4ekwvV7Ec9HpJqYMRJ4W8lirDDshHiQbf292abAv4WLJTu0M4hc6zcERab4MHiIGM4DQutF7kcZlGi4la7tiNanVOSVno3L3zcXRnJJlz3d0KQer66tIuAO0nGQDvGbLWiw_HYoo9eUPLms_cjplVeuh0coff7z5gxtYWQtbfPlUKUvh9437Hqb82Up9-lGIv6OhqxjmU-5xYdkwaTAxzq8xKW252muMP5R1O1HIrtpicHZ-kpNAykTKPG-H7UVU7AO0-VotwNMgDA7r4nuf-e0xkvyxBcM7pxuhnCsKPsgo4hRqkpfHGdeQHMG-blKveEw6WKgF6tphaWmiPecwY-NmG8wSHyeQ9s6JkEB4bxl_S8bW3VxuU_OCJU_lKtOXA7TjvyE5jmLP9n-bsomDR2gtfh9fgZ5BM8V8cDGavG2WgVts4eigen0XmgPuk3qQ5YM_2SSnm2u8eNiM5QwIVCzbr9LCcb2-rAswiIcs7gRoqBfwKJYWA0b4oivujpiieVniAhwq1cqn27mgg0fLkY6ZtC8LspIMJZZlTEgFWWE7VSihy3WfBg-sBlQ54GgaYjfcnDm9yE5yT60nQOoOwt7kB0de_oi9_-5BVBd6lTAxa_Q6sO9JRloR94_MVG93D-6BvD41_nz1V0jLfFB17rBDfS_4cw_25Mvy39NKBbKEMES4iJVUNNCaNWNDCY6VrmTdKt_AtotESENSTHryVDG23WDxZbcunz3hFLCG15t7kRXpLotnw9P0Opc8SlPW6yoh77TwpXyLKcseTL37LHLpsCXwc7PTmymD9pej0tasmmPcCfcxmG1Y1X1SPfYORFt0FXfNolPaXtUDjxtYZxz-sRQEtphDOsE39qlxHSKnHVixdBZIe3ne5JtiT4ARrNDzq52inQmWKZgDwGtYrhvGsmflys4yV861pJmpCkpW77Gsf6tKXTZnMBg3fZ6TeZixXVJLu0593qe_bSyJrBOyBgj-VEXdkMwvdG0eBY-wSgYTjAzshyucfBy1b-PkuektWQ6NzIFW9LcPk7_A4wDIEyIYK6W_fNKo76Uooa-VpT-zXbqONjHKqscUpPJh1knJtL1H9pDrg8Y5yi7M0hBZnfe6_n12NCDZwH3ZFF3owJ2RuJB4LTiKrHlV4ldm8ypCkKt4Gk-aS7g209wLeVQjt92KBti-a37qd3e8DxF9x-Jk7xZ5xBUlQtXgyYIaGDFtOucqfuCLele4_avQ1okJe9hjPmeUGaZAAqKcEvx5Q723RMoeJjizDUEyyGS5A2RCdsOF3byZ_IPHZG5jbItmMl-zQBwDKYWqxH9UJK7tG1ApLDsEgyJwQqHiPAjHRh0Bova58qgzUE9RCUp8rHkFf9ANntva-46Rob6h_BiVwhsGV_6D_WvUSWSW0rIHG9eako3JZPDoqtMPqri7c0pIR1o5zsidsLmN_zaBxhz_Ev5KQiwx-yBnd-kqGZsdATo4dwkPQRb1h232KAdQqWQMTmEKuKb2Z-caMFzDhGi5PE73mZgwKcL_eSKKXY59fPJfUXNdKBbk0QPy5LGamVOJVKKkIht7E9Atfc';

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
    var vehicleId = process.argv[2];
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

