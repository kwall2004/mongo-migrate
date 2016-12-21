var MongoClient = require('mongodb').MongoClient;
var request = require('request');
var elasticsearch = require('elasticsearch');
var Promise = require('bluebird');

// var uri = 'mongodb://localhost:27017/vision2';
var uri = 'mongodb://heroku_9d3ppsr0:a706flgp82q7qenmd166qmvq8d@ds051655.mlab.com:51655/heroku_9d3ppsr0';

var token = 'C7hCN1hOYUu1eBHBiHMIRln9fJ1Mm8tK0m_ZrIxu-T-jfFo2MvzXRy4RbQb-vv0zKlfpZWz1UAYavinB7L1HyM5x9f1uz-aPAUyW8zt4Ka0Pe2hCQ2p1seKHLuf_LQwigtqJHagg6TrdCW6rV0fcegxBIDFgQuT8Y8lNeLWfo5x0QylTcRRShkJgPYjYYwG93HbmDZz-i0anNUqR_bVXmgixTGLxOljVBxpjUNWEkWTqH1N8PdwPaRzBsmAAIlY2AeJb3fR2MLYpYEQVdGncU54DZwfn7sQbjPEPwVys4wIz5yUCUBDMM-a8YN-RgO2lKGgbOqrHOchp7ARRHJGiRx8j5hEgw6L9YufYv_IhtSSXx3pIf-2do9khsMNM_-iIjdLdUKFHs4l2DRvXYlArHrZWKY6KteB3Hdw6HlhDkLAzJTpQ7bkpqVJjA0HQsKNgqfYhUIx4tKZLz3VnhJ39daajE2zPVy2pN6M2zztNCaN4kWG404JAZ43ryK0pQrZF9sqDsDRyJdYXAbtqgyYoubt3ptv2w8VWzs6AyZo9TQyQwPXaJzazqT0_VwQ8g3CgzSs1jfXXo0knr6QS_uh-uMb72_xUImZhfBsIubmgJU2D4prFdK-iNWu4xbiGfncyjUBdpxnL1JaURja_pzY4jBTIvWH5fSgF-QCoakEyZeMJhW5bUX_zwqEHqnTmgcaY_k-sj2_RWbToDJDM_b1IC79Fr4N-cWndjq5YlG-rt-4FzL8a1JYXNzppsANxh1Fz00624daHgERFJ2wdmbH0DBGLnPUUGesEp8DwZNFZypeoc6ODuTBU92MM0LRH6f8_Xpv-VPFeO5g2U8Rs9WHL0_ULIQSqicEfJXl0-XIxTUWrIHv3_eJI439taEDeoiEGBoVDCEfYbOyTNN9TIdDHlUxWF3-W8A9PcJ8rdUceD9x3LabXj-6vFtB9zqSbSWhCjP7-hlrZQvGN7ONYbpt5Si81HjRvZ6XCaZaVsBdTmPXvOHw1nuRP3EPDIdKH_u7oG-gkxWkaa5BVdn4eeb4WWxLEy1LnXu89SGv4cBS47cj5zkU__9qasg3ld6ICIwG1COPJ7NQgnvOOpf6hdJ9iCVizV0Cs0LOG2FWcSUs9hIOcBrRw5m32WuAMlDlK9HmMiPcmWspsd1fgI7lgsS15feN7iStpSfP6MW4PxPVNjHPWTLf98RnwTCFPyhuvoRyXdKIsbs2uwqqbAJaYhUURefPkf9j8HxItf-F4JZgwVrxmnmTAf-mq1E8H2WeDqbsA52ttMSefggLx69rrNM63wAh-orh_g960OuEMR9n5P3eDBbCqj-nO8YjRgNH9qMy3UYPMoxj2INUQ5wOMm1FxjCL5yWQ4Q10St2l_zXyROCfD6XmzO0MN3TnYFTqj5qQIKyfxgCOB8hF5Yn769shZqSbu_NGH4nj65ADsDGqcTaeefIqDgZ0U17upTsg03eounkyD_cyNueIsykWkfq3RkVrqI-nEK9wTeIwC-sY4FrhssmnCCl9q6OsKLrwQMX9v4iy7dIUifrP5bVfy-SyRnnKWKz-Sx0IC0EthNHiflOs2sh1eZ8TxaJYk0YfHueRLNNTej8bv8_E43CLo50JFSg';

var client = new elasticsearch.Client({
  host: 'https://zix2f09c:89ujd04zflm5n3xu@aralia-3045944.us-east-1.bonsaisearch.net'
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

