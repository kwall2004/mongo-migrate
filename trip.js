var MongoClient = require('mongodb').MongoClient;
var request = require('request');
var elasticsearch = require('elasticsearch');

// var uri = 'mongodb://localhost:27017/vision2';
var uri = 'mongodb://heroku_9d3ppsr0:a706flgp82q7qenmd166qmvq8d@ds051655.mlab.com:51655/heroku_9d3ppsr0';

var token = 'YpSUUyZA0fmLmn_KHmBXdxvM2Gn5NUllNoLo5g-Bvgd4szG1FDfH6GcELIpHY96semBbkF0ZqEttE5c5w2zhEgv35hLYK7651JruWuFyN_3klWL864Mow3JNoXST5jfSv1IIABH1A8F6RcQzrl5bWsUACV64XmIdh7JZBAl3d2SMm-JfAwW0zBuvT93UTV7tPNfxN3XuoEYfwS2EjUbpW5Q3GGemOiGF-N9iMX5BzWKka6vUFES6ZPjmb-1Xof0NhGcwQ3x0k2YU-fwsb7T9iJlfMV6dYEdVw7FF8mh9zEqgVDbV75EMQquicNxJLSievFAoDOmKCkMh5eH1lvT800MGxgdZDbjuZK4-bccYkrDZ7nGL3f3NQaGKjG87q6ITlG2ciOyhkfACE8m-kS67ISRc73WjFFMlNDdTI-z3AN-6YCop_GDA5An3NZT7X6Uj2GsH5L9d9Isu5NpVWoBoi5opivqmQi3XL-chg5dDldmxLzsmOtUnK-PAfnZTBMLM0-e7ISgZozfWY2jTCMTFnd9YGpbOh8jhOIPXtQWDs2U9oo4e6GpLECTF4qK-1p4OawyQGvU2_DvEW_XLBjnW6KrqOQq2wexgcPfQyiD76WqD81S9uKm9RZvEJ-HxOdvz0uZlpvpbQ9tHJnlWlZm962_gRgrAloyIUwALFz0Hc_EBVyEPXZj65V0TTgrg8nwrOtYJx_7Ec81t18dUuFc78yq4W_JYJLmMRnYpQCx6cbVxzh89Z0nihkPNCynJR1pTF_IIBMtWYFT9kgHVb_RjmLe0Xd9tEDTsjDHFk7tBDxBiLGZjpGYgowgXL3zG4d_d-PPsqhZTcQz_C8_G29ZVvjuU1reC8Y7XPdjoVbloIPiscaOvuBtIBVGEL3FanjQZQ1AYOG0yJMbiDCG4I_lw2bwAjdSUr0yr4jgEpHtx1oKW9aR66JH4y5AVEjVcsbB2uZOHZsFLylqlZZJM001XRSrYC5-m6gEVLmihhqalAOfvSCl3Zcpf6MlsKyRSlqQN0Rbnz2ojPsfu2u5nwS-lpZi1DwbLxTHArEBzRMlmVfnA9VEp1O3GgEHP80TPY-PnLOjftp08fKnK01IEYKPL3oL--Q59UxWCKiiwsc0XiIiAcRs8IHt_OOzfHpNPjo7OKhEVM7TOtEhLtIHkpbFlJX_rrzN6tk_befV-r9YIH4wwTIxx96nf2RAd1x7ahZuRWwkKN8-LedYEEMs5rLZ1-7k-3kID2OLaQxPczoaYrmpHUSxYiO2kvJeq_95er501P80pnbng2cREKMDQ17pWorry1SXgWc6CaIWsoDBOsnHf5EG7uewTp4jaH63-7q4Ha36gFD9SOkKwk5AdCzzGxV4_qiPeCkSebdg_5ce2z-kT2JYeCvpbvB73M-Nw-pOYjymxo0AvRObCFyPgl7iXgACGhqqt-9md24W8og79zJ42BSymDfbpZ4C8UdEv_ybYyQde7VXKe_0orX63iTTCDTikraN4rt5rnMm8ZMlQEXE_jwZvggFdSVizNd_7TzqCHhVcHoYVpkMEbjdsSFzHPBuO9xmHmc9XAyX5RWUA-CnnXjY8weShT28Jtv8pSoqk';

var client = new elasticsearch.Client({
  host: 'https://lo0l35z0:6mnvwo0j46ih3gym@fig-7632293.us-east-1.bonsaisearch.net'
});

client.indices.exists(
  {
    index: 'vision'
  },
  function (error, response, status) {
    if (error) throw error;

    if (!response) {
      client.indices.create(
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
        },
        function (error) {
          if (error) throw error;

          migrate();
        }
      );
    }
    else {
      migrate();
    }
  }
);

function migrate() {
  // var vehicleId = '105342';
  var vehicleId = process.argv[2];
  var date = '2016-05-20';

  MongoClient.connect(uri, function (err, db) {
    if (err) throw err;

    var vehicles = db.collection('vehicles');

    vehicles.findOne({ oldId: parseInt(vehicleId) }, function (err, vehicle) {
      if (err) throw err;

      if (vehicle) {
        request.get(
          {
            url: 'http://visiontest.danlawinc.com/api/Trips/GetTrips?VehicleId=' + vehicleId + '&SelectedDate=' + date,
            auth: {
              bearer: token
            }
          },
          function (error, response, body) {
            if (error) throw error;

            if (response.statusCode != 200) {
              console.log(response);
              return;
            }

            var trips = JSON.parse(body);

            function processTrip(trips, i) {
              if (i == trips.length) {
                console.log('done', vehicleId, date);
                return;
              }

              var trip = trips[i];

              console.log(trip.trip.tripId);
              request.get(
                {
                  url: 'http://visiontest.danlawinc.com/api/Trips/GetTripJsonData?TripId=' + trip.trip.tripId,
                  auth: {
                    bearer: token
                  }
                },
                function (error, response, body) {
                  if (error) throw error;

                  if (response.statusCode == 200) {
                    var data = JSON.parse(body);

                    function processElement(data, j) {
                      if (j == data.length) {
                        console.log('done', trip.trip.tripId);
                        processTrip(trips, i + 1);
                        return;
                      }

                      var element = data[j];

                      var body = Object.assign(
                        {
                          tripId: trip.trip.tripId.toString(),
                          vehicleId: vehicle._id.toString(),
                          tripNumber: trip.trip.tripNumber.toString()
                        },
                        element
                      );
                      // console.log(body);
                      client.index(
                        {
                          index: 'vision',
                          type: 'message',
                          body: body
                        },
                        function (error, response) {
                          if (error) throw error;

                          console.log(j);

                          processElement(data, j + 1);
                        }
                      );
                    }

                    processElement(data, 0);
                  }
                }
              );
            }

            processTrip(trips, 0);
          }
        );
      }
    });
  });
}
