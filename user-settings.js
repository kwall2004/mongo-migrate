var MongoClient = require('mongodb').MongoClient;
var uri = 'mongodb://localhost:27017/vision2';
// var uri = 'mongodb://heroku_9d3ppsr0:a706flgp82q7qenmd166qmvq8d@ds051655.mlab.com:51655/heroku_9d3ppsr0';

MongoClient.connect(uri, function (err, db) {
  if (err) {
    console.log(err);
    return;
  }

  console.log('connected');
  var users = db.collection('users');
  var settings = db.collection('user-settings');
  var vehicles = db.collection('vehicles');
  settings.find().forEach(
    function (setting) {
      users.findOne({ OldId: setting.UserId }, function (err, user) {
        if (err) {
          console.log(err);
          return;
        }

        if (user) {
          settings.updateOne({ _id: setting._id }, { $set: { UserId: user._id.toString() } }, function (err, result) {
            if (err) {
              console.log(err);
              return;
            }

            console.log(result.result);
          });
        }
      });

      if (!isNaN(setting.Key)) {
        vehicles.findOne({ OldId: setting.Key }, function (err, vehicle) {
          if (err) {
            conole.log(err);
            return;
          }

          if (vehicle) {
            settings.updateOne({ _id: setting._id }, { $set: { Key: setting.Type, VehicleId: vehicle._id } }, function (err, result) {
              if (err) {
                console.log(err);
                return;
              }

              console.log(result.result);
            });
          }
        });
      }
      else if (setting.Key.startsWith('Default')) {
        var key = setting.Key.replace('Default', '');
        settings.updateOne({ _id: setting._id }, { $set: { Key: key } }, function (err, result) {
          if (err) {
            console.log(err);
            return;
          }

          console.log(result.result);
        });
      }

      settings.updateOne({ _id: setting._id }, { $unset: { Type: '' } }, function (err, result) {
        if (err) {
          console.log(err);
          return;
        }

        console.log(result.result);
      });
    },
    function (err) {
      if (err) {
        console.log(err);
      }
    }
  );
})
