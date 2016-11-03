var request = require('request');
var elasticsearch = require('elasticsearch');

var token = '2F31yfZyfvphkFxidnx2jPr732V3Y1U7w0EqzzMd5Rx_29OR_CI0rvO5Yzz3wxpnRZv6DRZNiBusdB3TslnZ9eLTWOCea6lIHMKJLPYdlWw4lxJTy71Tv0IpVOJjB6Bc0PVfqDTwOedr82PcfwmVSkIEB7IIY7yULMABaalr1uuQJ1JxtQTL2L4VDQel8govWz4gSG5pxj0fZhjTRMRU230aD6B_HUVTkfZP9NU4O7VwE-VgeM334n-D3nqnWWiDklLj5zSJsg-T9IP2jri32asmWt66nOOkhP7mYTXtEx4szwwBiuAvUdd62uDlwoefy0JiCRcqnYmoy5DeIeyxwBxZJEcxgH3XOpcR-0p55dGGMvxafvLQFbETqZVRQCqGWgX27At0FyGD4VEwbkZBKHbhOR839-LP2n38J02b9oWWk9iQ02rG8KtIYvam-G8oCk1cjllYT-8_EuYYivI7ADJTo_npVIcPikYdQypxIlfuMvEBI6Jc7duCzAV5ZYuFJWGz_mRsMe-w6ICXWtgRLvzJN3xQ9jySX7TIFYodxALFEHw9S-YHgCDz2kId1ZbvDwO74jV0sA-HiPjzcWzMyZ4KfyDvKTVTUi_3QrA6zYPkDx2pdosRDe4hq65qr5QE53tjsXO9hIiHfS_G5gQ3eDhzykVEncJwZEIyLhhhZJ5dAqLgKXViOkU2xHuoicX2BcUWCTbepzzRwjOuzICY5f_tM6e6oJoe8PSS-hIr-S8L7gBWkbXNmvnrY8oB_-OM5S346-8u4nFOVg2O-fJOQgXr5mUlfrsjFCKWpJYKRxwlg5RtQ0UapxsjPrtx2MPJqdwpbA8S5YSJ5EC9r3EOgkGlfaXrKFaVSHF0Pw8nVbqm2QA-7RAxGf3I1YY-hFBRoGPHWj5O0i2ayKkwf5TKyf0vN0P7L1X2lv0GYphrpX5jcgVz9POOuBCDsalgPxiutVi2Tz6kkDJCYFI9-UhIgbvUd52DIVSLdYqw2KwsP-YKyf49rTzFRPDqqlV7UX0L0ndsmXHTUVy-nijZLluf-Ov2bAxT0i1yVmAZSMT1qLQ-_pXVP61HlulXsMMmLv_OFMhxrM4QsCU4XqkFpcQeUBmNxxUtjqihvb89YQYuZ6PxoweMjFtpq-ZsosUe2A5pUXTvSL_rySY5tz_NyRFpKswltt9mxKRDvGXfob2xo0AjmfFt1rVQfnNEFT0QSs9g1pWuYvA9-V4rFUGgwOp0TSJq3ABYimmXTEL3wqy6HzMLnbFXRtQ1A0xl8s6rjL4XCfvIjFpwxn4UCAr_2OA7RSOYkqecVz4HKnCHvLhQ21i1Rba3dfmFOhNC4DcIKE2AXtWU9H3W7hx7e3QlWpQOrKolQhbY0q5wWVsAGHtQvot3nfiZA0g8GXLx4F_zdys9UUVbPiQreFbe9ZzavYtdZSUsOv1gGfrWvwlUf19-PBvs9xGiL-5QgzlagJyqTktCE-gqkwp2UZEqJfRMNcT_83ikCfy5SejtQmIixGYQGNhJEKT2i_ux-27GHkehXmtlC-8HCT-sAWmy1O54UuVgZkHeW8kizm3vw79J1mJyq76T8RZfqk9r0u18t7gw1IAm';

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
  var vehicleId1 = '105342';
  var vehicleId2 = '581a365f5d5f735698a021bf';
  var date = '2016-05-20';

  request.get(
    {
      url: 'http://visiontest.danlawinc.com/api/Trips/GetTrips?VehicleId=' + vehicleId1 + '&SelectedDate=' + date,
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
          console.log('done', vehicleId1, date);
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
                    vehicleId: vehicleId2,
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
