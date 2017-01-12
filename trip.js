var MongoClient = require('mongodb').MongoClient;
var request = require('request');
var elasticsearch = require('elasticsearch');
var Promise = require('bluebird');

// var uri = 'mongodb://localhost:27017/vision2';
var uri = 'mongodb://vis2devmongo01.danlawinc.com:27017/local';

var token = 'Y6--e8Hpi2XwW0bWPSiPlZar9e3dw0YV6qp9gRtIyOmsJrm1m5-GLvZwCxo-aaswcv2Gi5lLnz0FI0O4D5t41i8tu-ASOI1IyMJ3l9MEhNJIbsbYyMKZ9uZL4eF7doNXn9dMapOP-nvmz3cdoTanV_ACTHlTL-HS3DaGJ9hQH5BgTjJGWpn5vl1vVs8t0YZrJYC2E0nXInUt55ITOdrK5EpNKwJoP7RtDPfLN2vcaToTGyrmo4DSz_0WFUss1ze0fydvFrdU6W3sCfhl1ABzqB90su_LDARiSllAd0UVlFZRdg5QQNGTAw0zCc3grHeu1aQ4QXqGMaQ0LsMNwbleyU6IYpaCDcYm4kBf54BoFw7GUq84ei3iMuINRg-BrzkKrsU0MO-eLMEvxRbG3po_b49-OpoRGOGLur3dgWaVNvTBsHjbZE29YOwmxka6TtVDE99Ge7W7YanFyQf9OccodppGFlAxAIVH9x9f3X5MGpPrE6c8e12_zSzXLkxCyozdteIkMMWzxIOIJSHP_yKuVzDfWGs6fV04fBpq6RTf22_FwqVKO1morPbAq2W_xE7vwJpTADudEbmR9gxnkGIpL_f7GRE9o5nSBnTcFlZY6LmQwnk-bf_shhktMSq0EzQW-HV57HiJku2WMlfB-XU2PTVUyiQ-EHog3odGdVjF8u2i1aX5cYS7oDBNsOgcyJbGxaRcLWF9DLOuGZYensnBQ4vHSrd3b3_NrQ_pdR99qg8-tFZFmLuT74FMR3gnPgGXmh8sf823WsOqqMpStKR3B6ny17ICPtz0rvCXlNvXNXUC629iBUM2B8J0D05mx3yymdbWG9VCy5ZFD2-s5mkwD-xMY__BBcNJKGxe82vxM9jj9UnUP3mPXOR3URdOvd2bPh8IWxhvQfRmKCrw65JPSqQ8ia43k8YqeGz383Tlq1pbndubanlDZKKP8E_JUJZ9r6KmSzzDw9Hz-nX6blVvl9BK_Ca2FIESLsDM1RLhCSn6IkkK0YBh7jrcMn30kvE9mvoYmbI5sqxqoba_axIJ06K3XKorf2sCvMSISc3zXOlRMUxBScIF3C24EKpV6c9FF2NH4UmOEWFMnnua8RcgVusWObWxkHawDaPKHCfSDRGCD5n0hOxvuxF8YkQp0g1Q5k59be9QmNnZxg9W9XDN36Th8mtX9rEg17roBiAN27LnaHfbxpcsggZ1iE43vdjFgqeTW9i_9hSyqJU2w49NmMjAufkwn58hxbHqZZCsDHTwYr33RvAWoDlQj5VhVaI68KwYQBafbuI9H9e_wzW1-bu7Pvox6qx6jtvNzxHWCHOFEYir466kXUY_kGQlWE5VVnqIIhsEmsx3zMgeabDucEMGqxTLPl9T8yhpKwOQVBfh5AxB9ph--wMua382ZfLEJeH54ucLGL7jM3t0STEFrWnMDsHM1aoLKpc8-m5Zt5PuIPerAeOZisFiHLcDtmxiFqJqUsMwAM0RHxIvrWEH1eOJQ7-oxbmL9CV_uh6rbwWuY0F-X7aEbEPrfIxjiwMI_844sWysNd4iDdTMVMkZlqxClrfRpc4xwt7lXxS9uDuiKinRVD89Cz_pFrAzWFrMR_X9IZ_89Yprj1IIoM1_hg';

var client = new elasticsearch.Client({
  host: 'http://vis2develastic01.danlawinc.com:9200'
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

