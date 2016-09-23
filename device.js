var MongoClient = require('mongodb').MongoClient;
// var uri = 'mongodb://localhost:27017/vision2';
var uri = 'mongodb://heroku_9d3ppsr0:a706flgp82q7qenmd166qmvq8d@ds051655.mlab.com:51655/heroku_9d3ppsr0';

MongoClient.connect(uri, function (err, db) {
  if (err) {
    console.log(err);
    return;
  }

  console.log('connected');
  var clients = db.collection('clients');
  var devices = db.collection('devices');
  devices.find().forEach(
    function (device) {
      clients.findOne({ OldId: device.ClientId }, function (err, client) {
        if (err) {
          console.log(err);
          return;
        }

        if (client) {
          devices.updateOne({ _id: device._id }, { $set: { ClientId: client._id } }, function (err, result) {
            if (err) {
              console.log(err);
              return;
            }

            console.log(result.result);
          });
        }
      });

      devices.updateOne({ _id: device._id }, { $set: { IMEI: device.IMEI.toString() }});
      devices.updateOne({ _id: device._id }, { $set: { SerialNumber: device.SerialNumber.toString() }});
      devices.updateOne({ _id: device._id }, { $set: { GroupId: parseInt(device.GroupId) }});
    },
    function (err) {
      if (err) {
        console.log(err);
      }
    }
  );
})
